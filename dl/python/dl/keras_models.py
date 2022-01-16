#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

try:
    from debug.debug_tools import plpy
except ImportError:
    import plpy
import tensorflow as tf
from .conf import *
from .utils import *
from tensorflow.keras import backend as KTF
from .train_logger import TrainLogger
from .components_map import *


def _config_session():
    """
    plpython will hold GPU memory until session closed.
    So turn on `allow_growth` to avoid too much memory usage
    at initial time
    """
    config = tf.ConfigProto()
    config.gpu_options.allow_growth = True
    session = tf.Session(config=config)
    KTF.set_session(session)


def _def_model(input_dim, layers_def, losses, optimizer, metrics):
    model = tf.keras.models.Sequential()
    input_layer = layers_def[0]
    params = str2dict(input_layer[1])
    params['input_dim'] = input_dim
    model.add(layers_map[input_layer[0].lower()](**params))
    for layer_def in layers_def[1:]:
        model.add(layers_map[layer_def[0].lower()](**str2dict(layer_def[1])))
    model.compile(
        loss=[losses_map[loss[0].lower()](**str2dict(loss[1])) for loss in losses],
        optimizer=optimizers_map[optimizer[0].lower()](**str2dict(optimizer[1])),
        metrics=[metrics_map[m[0].lower()](**str2dict(m[1])) for m in metrics]
    )
    return model


def _get_model_train_conf(model):
    optimizer = json.dumps(
        {"name": model.optimizer.__class__.__name__, "config": model.optimizer.get_config()}
    )
    losses = json.dumps(
        [{"name": loss.__class__.__name__, "config": loss.get_config()} for loss in model.loss_functions])
    metrics = json.dumps([{"name": m.__class__.__name__, "config": m.get_config()} for m in model.metrics])
    return {
        "optimizer": optimizer,
        "losses": losses,
        "metrics": metrics
    }


def _load_model(model_id, with_weight=True):
    # result = plpy.execute(
    #     "SELECT model_arch, save_path FROM model_info WHERE model_id = '%s' LIMIT 1" % model_id
    # )
    result = query_model_info(
        model_id,
        "model_arch",
        "save_path"
    )
    if len(result) < 1:
        raise RuntimeError("No such model, model_id: %s" % model_id)
    model = tf.keras.models.model_from_json(
        json.dumps(result[0]["model_arch"])
    )
    if with_weight:
        model.load_weights(result[0]["save_path"])
    return model


def _fit(model, data_func, epochs, steps_per_epoch, class_weight, validation_steps, log_table, save_path):
    for epoch in xrange(epochs):
        data = data_func()
        model.fit(
            x=data,
            epochs=epoch + 1,
            initial_epoch=epoch,
            steps_per_epoch=steps_per_epoch,
            callbacks=[TrainLogger(log_table)],
            workers=0,  # execute the generator on the main thread to ensure `steps_per_epoch` work correctly
            validation_data=data,
            validation_steps=validation_steps,
            class_weight=class_weight
        )
    model.save_weights(save_path)


def _train(model, model_category, train_table, x_columns, y_column, epochs, batch_size, steps_per_epoch, class_weight,
           log_table, save_path, valid_data, p_model_id=None):

    def _write_keras_model_info():
        arch_ = model.to_json()
        write_model_info(model_id, model_category, p_model_id, arch_, train_conf["optimizer"], train_conf["losses"],
                          train_conf["metrics"], train_table, x_columns, y_column, valid_data, epochs, batch_size,
                          steps_per_epoch, json.dumps(class_weight), validation_steps, save_path, log_table)

    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE
    if epochs is None:
        epochs = DEFAULT_EPOCHS
    num_samples = plpy.execute("SELECT COUNT(1) FROM %s" % train_table)[0]["count"]
    if valid_data is None:
        valid_data = DEFAULT_VALIDATION
    validation_steps = int(num_samples * valid_data) / batch_size
    if steps_per_epoch is None:
        train_steps = int((1 - valid_data) * num_samples) / batch_size + 1
    else:
        train_steps = steps_per_epoch
        validation_steps = max(0, num_samples/batch_size + 1 - train_steps)
    model_id = gen_model_id()
    save_path = get_save_path(save_path, model_id)
    log_table = get_log_table(log_table, model_id)
    create_log_table(log_table)
    data_func = lambda: data_generator(train_table, x_columns, y_column, batch_size)
    if class_weight is not None:
        class_weight = str2dict(class_weight)
        for k in class_weight.keys():
            class_weight[int(k)] = class_weight.pop(k)
    # get model info before `fit` to avoid json dumps error
    train_conf = _get_model_train_conf(model)
    _fit(model, data_func, epochs, train_steps, class_weight, validation_steps, log_table, save_path)
    _write_keras_model_info()
    return model_id


def train(layers_def, losses, optimizer, metrics, train_table,
          x_columns, y_column, epochs, batch_size, steps_per_epoch, class_weight,
          log_table, save_path, valid_data, **kwargs):
    # config session at first to avoid un-initialized error
    _config_session()
    input_dim = len(x_columns)
    if layers_def is None:
        layers_def = DEFAULT_LAYER_DEF
    if losses is None:
        losses = DEFAULT_LOSS
    if optimizer is None:
        optimizer = DEFAULT_OPTIMIZER
    if metrics is None:
        metrics = DEFAULT_METRICS
    model = _def_model(input_dim, layers_def, losses, optimizer, metrics)
    model_category = kwargs.get("model_category", MODEL_CATEGORY["keras_udm"])
    return _train(model, model_category, train_table, x_columns, y_column,
                  epochs, batch_size, steps_per_epoch, class_weight,
                  log_table, save_path, valid_data)


def train_from(model_id, losses, optimizer, metrics, train_table,
               x_columns, y_column, epochs, batch_size, steps_per_epoch, class_weight,
               log_table, save_path, valid_data, **kwargs):
    # config session at first to avoid un-initialized error
    _config_session()
    model = _load_model(model_id)
    result = query_model_info(
        model_id,
        "losses",
        "optimizer",
        "metrics",
        "x_columns",
        "y_column"
    )
    if losses is None:
        losses = [losses_map[loss["name"].lower()].from_config(loss["config"])
                  for loss in result[0]["losses"]]
    else:
        losses = [losses_map[loss[0].lower()](**str2dict(loss[1])) for loss in losses]
    if optimizer is None:
        optimizer = optimizers_map[result[0]["optimizer"]["name"].lower()].from_config(result[0]["optimizer"]["config"])
    else:
        optimizer = optimizers_map[optimizer[0].lower()](**str2dict(optimizer[1]))
    if metrics is None:
        metrics = [metrics_map[m["name"].lower()].from_config(m["config"])
                   for m in result[0]["metrics"]]
    else:
        metrics = [metrics_map[m[0].lower()](**str2dict(m[1])) for m in metrics]
    if x_columns is None:
        x_columns = result[0]["x_columns"]
    if y_column is None:
        y_column = result[0]["y_column"]
    model.compile(loss=losses, optimizer=optimizer, metrics=metrics)
    model_category = kwargs.get("model_category", MODEL_CATEGORY["keras_udm"])
    return _train(model, model_category, train_table, x_columns, y_column,
                  epochs, batch_size, steps_per_epoch, class_weight, log_table, save_path,
                  valid_data, p_model_id=model_id)


def train_linear_regression(train_table, x_columns, y_column, epochs, batch_size, valid_data,  **kwargs):
    layers_def = [["dense", "units=1"], ]
    return train(
        layers_def=layers_def,
        losses=None,
        optimizer=None,
        metrics=None,
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        epochs=epochs,
        batch_size=batch_size,
        steps_per_epoch=None,
        class_weight=None,
        log_table=None,
        save_path=None,
        valid_data=valid_data,
        model_category=MODEL_CATEGORY["linear_regression"]
    )


def train_logistic_regression(train_table, x_columns, y_column, epochs, batch_size, class_weight, valid_data,  **kwargs):
    layers_def = [["dense", "units=2"], ]
    losses = [['sparsecategoricalcrossentropy', 'from_logits=1'], ]
    metrics = [['sparsecategoricalaccuracy', ''], ]
    return train(
        layers_def=layers_def,
        losses=losses,
        optimizer=None,
        metrics=metrics,
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        epochs=epochs,
        batch_size=batch_size,
        steps_per_epoch=None,
        class_weight=class_weight,
        log_table=None,
        save_path=None,
        valid_data=valid_data,
        model_category=MODEL_CATEGORY["logistic_regression"]
    )


def train_neural_network_classifier(
        hidden_units,
        train_table,
        x_columns,
        y_column,
        epochs,
        batch_size,
        class_weight,
        valid_data,
        **kwargs):
    layers_def = [["dense", "units=%d,activation=relu" % units] for units in hidden_units]
    out_dims = plpy.execute(
        "SELECT COUNT(DISTINCT %s) AS out_dims FROM %s" % (y_column, train_table)
    )[0]["out_dims"]
    layers_def.append(["dense", "units=%d" % out_dims])
    losses = [['sparsecategoricalcrossentropy', 'from_logits=1'], ]
    metrics = [['sparsecategoricalaccuracy', ''], ]
    return train(
        layers_def=layers_def,
        losses=losses,
        optimizer=None,
        metrics=metrics,
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        epochs=epochs,
        batch_size=batch_size,
        steps_per_epoch=None,
        class_weight=class_weight,
        log_table=None,
        save_path=None,
        valid_data=valid_data,
        model_category=MODEL_CATEGORY["neural_network_classifier"]
    )


def train_neural_network_regressor(
        hidden_units,
        train_table,
        x_columns,
        y_column,
        epochs,
        batch_size,
        valid_data,
        **kwargs):
    layers_def = [["dense", "units=%d,activation=tanh" % units] for units in hidden_units]
    layers_def.append(["dense", "units=1"])
    return train(
        layers_def=layers_def,
        losses=None,
        optimizer=None,
        metrics=None,
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        epochs=epochs,
        batch_size=batch_size,
        steps_per_epoch=None,
        class_weight=None,
        log_table=None,
        save_path=None,
        valid_data=valid_data,
        model_category=MODEL_CATEGORY["neural_network_regressor"]
    )


def predict(model_id, data_table, id_column, x_columns, batch_size, result_table, result_type, **kwargs):
    # config session at first to avoid un-initialized error
    _config_session()
    model = _load_model(model_id)
    if result_type == "probability":
        predict_col_type = "REAL[]"
    else:
        predict_col_type = "INTEGER"
    if result_table is None:
        result_table = model_id + "_preds_on_" + data_table.replace('.', '-')
    if x_columns is None:
        x_columns = query_model_info(model_id, "x_columns")[0]["x_columns"]
    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE
    # TODO remove me
    # plpy.execute("DROP TABLE IF EXISTS %s" % result_table)
    # iter = 0
    plpy.execute(
        "CREATE UNLOGGED TABLE %s(id VARCHAR, predict %s) DISTRIBUTED BY (id)" % (result_table, predict_col_type))
    insert_sql = "INSERT INTO %s VALUES " % result_table
    for x, ids in data_generator(data_table, x_columns, id_column, batch_size):
        # print(iter, time.ctime())
        # iter += 1
        preds = model.predict(x, batch_size=batch_size)
        if result_type != "probability": preds = preds.argmax(axis=-1).ravel()
        plpy.execute(insert_sql + str(zip(ids.tolist(), preds.tolist())).strip('[]').replace('[', 'ARRAY['))
    return result_table


def evaluate(model_id, metrics, data_table, x_columns, y_column, batch_size, steps, **kwargs):
    # config session at first to avoid un-initialized error
    _config_session()
    model = _load_model(model_id)
    result = query_model_info(
        model_id,
        "losses",
        "optimizer",
        "metrics",
        "x_columns",
        "y_column"
    )
    losses = [losses_map[loss["name"].lower()].from_config(loss["config"])
              for loss in result[0]["losses"]]
    optimizer = optimizers_map[result[0]["optimizer"]["name"].lower()].from_config(result[0]["optimizer"]["config"])
    if metrics is None:
        metrics = [metrics_map[m["name"].lower()].from_config(m["config"]) for m in result[0]["metrics"]]
    else:
        metrics = [metrics_map[m[0].lower()](**str2dict(m[1])) for m in metrics]
    if x_columns is None:
        x_columns = result[0]["x_columns"]
    if y_column is None:
        y_column = result[0]["y_column"]
    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE
    if steps is None:
        steps = plpy.execute("SELECT COUNT(1) FROM %s" % data_table)[0]["count"] / batch_size + 1
    model.compile(optimizer=optimizer, loss=losses, metrics=metrics)
    return model.evaluate(
        x=data_generator(data_table, x_columns, y_column, batch_size),
        workers=0,
        steps=steps
    )[len(losses):]
