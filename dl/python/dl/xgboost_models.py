#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

try:
    from debug.debug_tools import plpy
except ImportError:
    import plpy
import xgboost as xgb
from itertools import product
from .conf import *
import pandas as pd
import gc
from .utils import *


def _xgboost_train(
        train_table,
        x_columns,
        y_column,
        model_category,
        num_boost_round,
        objective,
        num_class,
        scale_pos_weight):

    def _write_train_log():
        plan = plpy.prepare(
                    "INSERT INTO %s(epoch, logs) VALUES($1, $2)" % log_table,
                    ["integer", "text"]
                )
        keys1 = evals_result.keys()
        keys2 = evals_result[keys1[0]].keys()
        for epoch in xrange(num_boost_round):
            try:
                log = {"%s-%s" % (k1, k2): evals_result[k1][k2][epoch] for k1, k2 in product(keys1, keys2)}
                plpy.execute(plan, [epoch, str(log)])
            except IndexError:
                break

    def _write_xgboost_model_info():
        if model_category in (MODEL_CATEGORY["boosting_tree_classifier"], MODEL_CATEGORY["boosting_tree_regressor"]):
            param["best_ntree_limit"] = bst.best_ntree_limit
        arch_ = json.dumps(param)
        write_model_info(model_id, model_category, None, arch_, None, None, None,
                          train_table, x_columns, y_column, valid_data, num_boost_round, None,
                          None, None, None, save_path, log_table)
    param = {
        'max_depth': 5,
        'eta': .2,
        'colsample_bytree': .75,
        'colsample_bylevel': .75,
        'colsample_bynode': .75,
        'objective': objective,
        'tree_method': 'gpu_hist' if GPU_AVAILABLE else 'hist'
    }
    if num_class is not None and num_class > 2:
        param["num_class"] = num_class
    if scale_pos_weight is not None:
        param['scale_pos_weight'] = scale_pos_weight
    num_samples = plpy.execute(
        "SELECT COUNT(1) AS num_samples FROM %s" % train_table
    )[0]["num_samples"]
    df = pd.DataFrame(plpy.execute("SELECT %s FROM %s" % (','.join(x_columns+[y_column, ]), train_table))[:])
    valid_data = DEFAULT_VALIDATION
    dtrain = xgb.DMatrix(
        data=df[:int(num_samples-num_samples*valid_data)][x_columns],
        label=df[:int(num_samples-num_samples*valid_data)][y_column]
    )
    dvalid = xgb.DMatrix(
        data=df[int(num_samples - num_samples * valid_data):][x_columns],
        label=df[int(num_samples - num_samples * valid_data):][y_column]
    )
    del df
    gc.collect()
    early_stopping_rounds = 5
    if model_category in (MODEL_CATEGORY['random_forest_classifier'], MODEL_CATEGORY['random_forest_regressor']):
        num_boost_round = 1
        early_stopping_rounds = None
        num_trees = max(10, min(100, num_samples / 1024))
        param['num_parallel_tree'] = num_trees
    model_id = gen_model_id()
    log_table = get_log_table(None, model_id)
    create_log_table(log_table)
    save_path = get_save_path(None, model_id)
    evals_result = dict()
    bst = xgb.train(
        param,
        dtrain,
        num_boost_round=num_boost_round,
        evals=[(dtrain, 'train'), (dvalid, 'valid')],
        early_stopping_rounds=early_stopping_rounds,
        evals_result=evals_result
    )
    bst.save_model(save_path)
    _write_train_log()
    _write_xgboost_model_info()
    return model_id


def train_random_forest_regressor(
        train_table,
        x_columns,
        y_column,
        **kwargs):
    return _xgboost_train(
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        model_category=MODEL_CATEGORY["random_forest_regressor"],
        num_boost_round=1,
        objective="reg:linear",
        num_class=None,
        scale_pos_weight=None
    )


def train_random_forest_classifier(
        train_table,
        x_columns,
        y_column,
        positive_class_weight=None,
        **kwargs):
    num_class = plpy.execute(
        "SELECT COUNT(DISTINCT %s) AS num_class FROM %s" % (y_column, train_table)
    )[0]["num_class"]
    scale_pos_weight = None
    if num_class == 2:
        objective = 'binary:logistic'
        scale_pos_weight = positive_class_weight
    elif num_class > 2:
        objective = 'multi:softmax'
    else:
        raise RuntimeError("`%s` have only one distinct value" % y_column)

    return _xgboost_train(
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        model_category=MODEL_CATEGORY["random_forest_classifier"],
        num_boost_round=1,
        objective=objective,
        num_class=num_class,
        scale_pos_weight=scale_pos_weight
    )


def train_boosting_tree_regressor(
        train_table,
        x_columns,
        y_column,
        epochs,
        **kwargs):
    return _xgboost_train(
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        model_category=MODEL_CATEGORY["boosting_tree_regressor"],
        num_boost_round=epochs,
        objective="reg:linear",
        num_class=None,
        scale_pos_weight=None
    )


def train_boosting_tree_classifier(
        train_table,
        x_columns,
        y_column,
        epochs,
        positive_class_weight=None,
        **kwargs):
    num_class = plpy.execute(
        "SELECT COUNT(DISTINCT %s) AS num_class FROM %s" % (y_column, train_table)
    )[0]["num_class"]
    scale_pos_weight = None
    if num_class == 2:
        objective = 'binary:logistic'
        scale_pos_weight = positive_class_weight
    elif num_class > 2:
        objective = 'multi:softmax'
    else:
        raise RuntimeError("`%s` have only one distinct value" % y_column)

    return _xgboost_train(
        train_table=train_table,
        x_columns=x_columns,
        y_column=y_column,
        model_category=MODEL_CATEGORY["boosting_tree_classifier"],
        num_boost_round=epochs,
        objective=objective,
        num_class=num_class,
        scale_pos_weight=scale_pos_weight
    )


def predict(model_id, data_table, id_column, x_columns, batch_size, result_table):
    batch_size = batch_size if batch_size is not None else DEFAULT_BATCH_SIZE
    result_table = result_table if result_table is not None else model_id + "_preds_on_" + data_table.replace('.', '-')
    model_info = query_model_info(model_id, "model_arch", "model_category", "save_path", "x_columns")[0]
    model_category = model_info["model_category"]
    if model_category in (MODEL_CATEGORY["random_forest_classifier"], MODEL_CATEGORY["boosting_tree_classifier"]):
        predict_col_type = "INTEGER"
    elif model_category in (MODEL_CATEGORY["random_forest_regressor"], MODEL_CATEGORY["boosting_tree_regressor"]):
        predict_col_type = "REAL"
    else:
        predict_col_type = "INTEGER"
        pass
    plpy.execute(
        "CREATE UNLOGGED TABLE %s(id VARCHAR, predict %s) DISTRIBUTED BY (id)" % (result_table, predict_col_type))
    insert_sql = "INSERT INTO %s VALUES " % result_table
    if x_columns is None:
        x_columns = model_info["x_columns"]
    bst = xgb.Booster()
    bst.load_model(model_info["save_path"])
    ntree_limit = model_info["model_arch"].get("best_ntree_limit", 0)
    for x, ids in data_generator(data_table, x_columns, id_column, batch_size):
        # print(iter, time.ctime())
        # iter += 1
        preds = bst.predict(
            xgb.DMatrix(x, feature_names=x_columns),
            ntree_limit=ntree_limit
        )
        plpy.execute(insert_sql + str(zip(ids.tolist(), preds.tolist())).strip('[]'))
    return result_table


def evaluate(model_id, data_table, x_columns, y_column, batch_size):

    def _update_accumulated_result(err):
        accumulated_result[0] = accumulated_result[0] * accumulated_result[1] + err * num_samples
        accumulated_result[1] = accumulated_result[1] + num_samples
        accumulated_result[0] = accumulated_result[0] / accumulated_result[1]

    def _error_rate():
        error_rate = np.sum(np.asarray(preds.ravel() != y_true.ravel(), dtype=np.int)) * 1. / num_samples
        _update_accumulated_result(error_rate)

    def _rmse():
        rmse = np.sqrt(np.mean((preds.ravel() - y_true.ravel())**2))
        _update_accumulated_result(rmse)

    batch_size = batch_size if batch_size is not None else DEFAULT_BATCH_SIZE
    model_info = query_model_info(model_id, "model_arch", "model_category", "save_path", "x_columns", "y_column")[0]
    model_category = model_info["model_category"]
    if model_category in (MODEL_CATEGORY["random_forest_classifier"], MODEL_CATEGORY["boosting_tree_classifier"]):
        metric_func = _error_rate
    elif model_category in (MODEL_CATEGORY["random_forest_regressor"], MODEL_CATEGORY["boosting_tree_regressor"]):
        metric_func = _rmse
    else:
        metric_func = _rmse
    if x_columns is None:
        x_columns = model_info["x_columns"]
    if y_column is None:
        y_column = model_info["y_column"]

    bst = xgb.Booster()
    bst.load_model(model_info["save_path"])
    ntree_limit = model_info["model_arch"].get("best_ntree_limit", 0)

    accumulated_result = [.0, 0]
    for x, y_true in data_generator(data_table, x_columns, y_column, batch_size):
        # print(iter, time.ctime())
        # iter += 1
        preds = bst.predict(
            xgb.DMatrix(x, feature_names=x_columns),
            ntree_limit=ntree_limit
        )
        num_samples = y_true.shape[0]
        metric_func()

    return [accumulated_result[0], ]


if __name__ == "__main__":
    print(evaluate(
        "model_200011ca24203468be11c344e9aef22a",
        "iris_data",
        None, None, None
    ))
