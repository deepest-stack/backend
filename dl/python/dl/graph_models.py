#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

try:
    from debug.debug_tools import plpy
except ImportError:
    import plpy
from .gnn import FullGCN, SampledGCN
from .gnn import SupervisedSAGE, UnsupervisedSAGE
from .graph_embedding import LINE, DeepWalk
from .utils import *
import json
import tensorflow as tf
from .conf import MODEL_CATEGORY, DEFAULT_BATCH_SIZE
import time
from .multi_process import SeedProcess
from .multi_process import FullGCNSampler, SampledGCNSampler
from .multi_process import SupervisedSAGESampler, UnsupervisedSAGESampler
from .multi_process import DeepWalkSampler, LineSampler
from multiprocessing import Queue
from Queue import Empty


CLASS_MAP = {
    MODEL_CATEGORY["full_gcn"]: FullGCN,
    MODEL_CATEGORY["sampled_gcn"]: SampledGCN,
    MODEL_CATEGORY["supervised_sage"]: SupervisedSAGE,
    MODEL_CATEGORY["unsupervised_sage"]: UnsupervisedSAGE,
    MODEL_CATEGORY["deepwalk"]: DeepWalk,
    MODEL_CATEGORY["line"]: LINE
}


SAMPLER_MAP = {
    MODEL_CATEGORY["full_gcn"]: FullGCNSampler,
    MODEL_CATEGORY["sampled_gcn"]: SampledGCNSampler,
    MODEL_CATEGORY["supervised_sage"]: SupervisedSAGESampler,
    MODEL_CATEGORY["unsupervised_sage"]: UnsupervisedSAGESampler,
    MODEL_CATEGORY["deepwalk"]: DeepWalkSampler,
    MODEL_CATEGORY["line"]: LineSampler
}


class LoggerHook(tf.train.SessionRunHook):

    def __init__(self, log_table):
        super(LoggerHook, self).__init__()
        self._current_iter = 0
        create_log_table(log_table)
        self._plan = plpy.prepare(
            "INSERT INTO %s(epoch, batch, logs) VALUES($1, $2, $3)" % log_table,
            ["integer", "integer", "text"]
        )

    def before_run(self, run_context):
        return run_context.original_args

    def after_run(self, run_context, run_values):
        self._current_iter += 1
        print(time.ctime(), {"loss": run_values.results[1], "iteration": self._current_iter})
        plpy.execute(
            self._plan,
            [0, self._current_iter, str({"loss": run_values.results[1]})]
        )


def _query_class_num(node_type, nodes_def):
    label_column = None
    for node_def in json.loads(nodes_def):
        if node_type != node_def["node_type"]:
            continue
        label_column = node_def.get("label_column", "label")
    if not label_column:
        raise ValueError("Could not determine class num by `nodes_def`, specify it using `target_node_class_num`")
    plan = plpy.prepare(
            "SELECT COUNT(DISTINCT %s) AS class_num FROM public.vertices WHERE node_type = $1" % label_column,
            ["varchar", ]
        )
    return plpy.execute(plan, [node_type, ])[0]["class_num"]


def _get_features_num(nodes_def):
    features_num = dict()
    for node_def in json.loads(nodes_def):
        features_num[node_def["node_type"]] = len(node_def.get("attr_columns", []))
    return features_num


def _data_generator(result_queue, timeout):
    while True:
        try:
            yield result_queue.get(timeout=timeout)
        except Empty:
            break


def _train_graph_model(
        model_category,
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        result_queue,
        trainer_timeout,
        batch_size,
        epochs,
        **kwargs):
    model_id = gen_model_id()
    log_table = get_log_table(None, model_id)
    model_dir = get_save_path(None, model_id)

    model = CLASS_MAP[model_category](
        build_graph(nodes_def, edges_def, redis_host, redis_port, redis_passwd),
        mode="train",
        **kwargs
    )

    est = tf.estimator.Estimator(
        model_fn=model.model_fn,
        model_dir=model_dir
    )

    while result_queue.empty():
        time.sleep(0.1)

    est.train(
        input_fn=lambda: model.input_fn(lambda: _data_generator(result_queue, trainer_timeout)),
        hooks=[LoggerHook(log_table), ]
    )

    kwargs["nodes_def"] = nodes_def
    kwargs["edges_def"] = edges_def
    write_model_info(
        model_id=model_id,
        model_category=model_category,
        p_model_id=None,
        arch=json.dumps(kwargs),
        optimizer=None,
        losses=None,
        metrics=None,
        train_table=None,
        x_columns=None,
        y_column=None,
        valid_data=None,
        epochs=epochs,
        batch_size=batch_size,
        steps_per_epoch=None,
        class_weight=None,
        validation_steps=None,
        save_path=model_dir,
        log_table=log_table
    )
    return model_id


def _check_args(target_node_class_num, target_node_type, features_num):
    if not target_node_class_num:
        target_node_class_num = _query_class_num(target_node_type, nodes_def)
    if not features_num:
        features_num = _get_features_num(nodes_def)
    return target_node_class_num, features_num


def _create_seed_process(name,
                         task_queue,
                         nodes_def,
                         edges_def,
                         node_type,
                         redis_host,
                         redis_port,
                         redis_passwd,
                         init_redis,
                         batch_size,
                         epochs):
    return SeedProcess(
        name=name,
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=epochs
    )


def _join_process(*ps):
    for p in ps:
        if p.is_alive(): p.join()


def train_full_gcn(
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        target_node_type,
        sample_paths,
        target_node_class_num=None,
        features_num=None,
        categorical_attrs_desc=None,
        hidden_dim=16,
        hidden_act="relu",
        in_drop_rate=.0,
        use_input_bn=True,
        need_dense=True,
        dense_act=None,
        batch_size=64,
        epochs=10,
        init_redis=True,
        sampler_process_num=8,
        sampler_timeout=1,
        trainer_timeout=3,
        **kwargs):

    target_node_class_num, features_num = _check_args(target_node_class_num, target_node_type, features_num)

    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=target_node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=epochs
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            FullGCNSampler(
                name="full_gcn_sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                output_dim=target_node_class_num,
                features_num=features_num,
                node_type=target_node_type,
                sample_paths=sample_paths,
                categorical_attrs_desc=categorical_attrs_desc,
                hidden_dim=hidden_dim,
                hidden_act=hidden_act,
                in_drop_rate=in_drop_rate,
                use_input_bn=use_input_bn,
                need_dense=need_dense,
                dense_act=dense_act,
                mode="train",
                timeout=sampler_timeout,
                **kwargs
            )
        )
    for p in sampler_processes:
        p.start()

    model_id = _train_graph_model(
        MODEL_CATEGORY["full_gcn"],
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        result_queue,
        trainer_timeout,
        batch_size,
        epochs,
        output_dim=target_node_class_num,
        features_num=features_num,
        node_type=target_node_type,
        sample_paths=sample_paths,
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=hidden_dim,
        hidden_act=hidden_act,
        in_drop_rate=in_drop_rate,
        use_input_bn=use_input_bn,
        need_dense=need_dense,
        dense_act=dense_act
    )

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return model_id


def train_sampled_gcn(
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        target_node_type,
        sample_paths,
        neighs_num,
        target_node_class_num=None,
        features_num=None,
        categorical_attrs_desc=None,
        hidden_dim=16,
        hidden_act="relu",
        in_drop_rate=.0,
        use_input_bn=True,
        need_dense=True,
        dense_act=None,
        batch_size=64,
        epochs=10,
        init_redis=True,
        sampler_process_num=8,
        sampler_timeout=1,
        trainer_timeout=3,
        **kwargs):

    target_node_class_num, features_num = _check_args(target_node_class_num, target_node_type, features_num)

    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=target_node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=epochs
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            SampledGCNSampler(
                name="sampled_gcn_sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                output_dim=target_node_class_num,
                features_num=features_num,
                node_type=target_node_type,
                sample_paths=sample_paths,
                neighs_num=neighs_num,
                categorical_attrs_desc=categorical_attrs_desc,
                hidden_dim=hidden_dim,
                hidden_act=hidden_act,
                in_drop_rate=in_drop_rate,
                use_input_bn=use_input_bn,
                need_dense=need_dense,
                dense_act=dense_act,
                mode="train",
                timeout=sampler_timeout,
                **kwargs
            )
        )
    for p in sampler_processes:
        p.start()

    model_id = _train_graph_model(
        MODEL_CATEGORY["sampled_gcn"],
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        result_queue,
        trainer_timeout,
        batch_size,
        epochs,
        output_dim=target_node_class_num,
        features_num=features_num,
        node_type=target_node_type,
        sample_paths=sample_paths,
        neighs_num=neighs_num,
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=hidden_dim,
        hidden_act=hidden_act,
        in_drop_rate=in_drop_rate,
        use_input_bn=use_input_bn,
        need_dense=need_dense,
        dense_act=dense_act,
    )

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return model_id


def train_supervised_sage(
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        target_node_type,
        sample_paths,
        neighs_num,
        agg_type="gcn",
        target_node_class_num=None,
        features_num=None,
        categorical_attrs_desc=None,
        hidden_dim=16,
        hidden_act="relu",
        in_drop_rate=.0,
        use_input_bn=True,
        need_dense=True,
        dense_act=None,
        batch_size=64,
        epochs=10,
        init_redis=True,
        sampler_process_num=8,
        sampler_timeout=1,
        trainer_timeout=3,
        **kwargs):

    target_node_class_num, features_num = _check_args(target_node_class_num, target_node_type, features_num)

    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=target_node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=epochs
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            SupervisedSAGESampler(
                name="supervised_sage_sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                output_dim=target_node_class_num,
                features_num=features_num,
                node_type=target_node_type,
                sample_paths=sample_paths,
                neighs_num=neighs_num,
                agg_type=agg_type,
                categorical_attrs_desc=categorical_attrs_desc,
                hidden_dim=hidden_dim,
                hidden_act=hidden_act,
                in_drop_rate=in_drop_rate,
                use_input_bn=use_input_bn,
                need_dense=need_dense,
                dense_act=dense_act,
                mode="train",
                timeout=sampler_timeout,
                **kwargs
            )
        )
    for p in sampler_processes:
        p.start()

    model_id = _train_graph_model(
        MODEL_CATEGORY["supervised_sage"],
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        result_queue,
        trainer_timeout,
        batch_size,
        epochs,
        output_dim=target_node_class_num,
        features_num=features_num,
        node_type=target_node_type,
        sample_paths=sample_paths,
        neighs_num=neighs_num,
        agg_type=agg_type,
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=hidden_dim,
        hidden_act=hidden_act,
        in_drop_rate=in_drop_rate,
        use_input_bn=use_input_bn,
        need_dense=need_dense,
        dense_act=dense_act,
    )

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return model_id


def train_unsupervised_sage(
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        target_node_type,
        sample_paths,
        positive_sample_path,
        neighs_num,
        neg_num=10,
        agg_type="gcn",
        features_num=None,
        categorical_attrs_desc=None,
        hidden_dim=16,
        hidden_act="relu",
        in_drop_rate=.0,
        use_input_bn=True,
        need_dense=True,
        dense_act=None,
        batch_size=64,
        epochs=10,
        init_redis=True,
        sampler_process_num=8,
        sampler_timeout=1,
        trainer_timeout=3,
        **kwargs):

    if not features_num:
        features_num = _get_features_num(nodes_def)

    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=target_node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=epochs
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            UnsupervisedSAGESampler(
                name="unsupervised_sage_sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                features_num=features_num,
                node_type=target_node_type,
                sample_paths=sample_paths,
                positive_sample_path=positive_sample_path,
                neighs_num=neighs_num,
                agg_type=agg_type,
                categorical_attrs_desc=categorical_attrs_desc,
                hidden_dim=hidden_dim,
                hidden_act=hidden_act,
                in_drop_rate=in_drop_rate,
                use_input_bn=use_input_bn,
                need_dense=need_dense,
                dense_act=dense_act,
                mode="train",
                timeout=sampler_timeout,
                **kwargs
            )
        )
    for p in sampler_processes:
        p.start()

    model_id = _train_graph_model(
        MODEL_CATEGORY["unsupervised_sage"],
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        result_queue,
        trainer_timeout,
        batch_size,
        epochs,
        features_num=features_num,
        node_type=target_node_type,
        sample_paths=sample_paths,
        positive_sample_path=positive_sample_path,
        neighs_num=neighs_num,
        neg_num=neg_num,
        agg_type=agg_type,
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=hidden_dim,
        hidden_act=hidden_act,
        in_drop_rate=in_drop_rate,
        use_input_bn=use_input_bn,
        need_dense=need_dense,
        dense_act=dense_act,
    )

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return model_id


def train_deepwalk(
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        node_type,
        node_count,
        edge_type,
        walk_len=10,
        window_size=5,
        embedding_dim=64,
        neg_num=5,
        temperature=1.0,
        str2hash=False,
        batch_size=64,
        epochs=10,
        init_redis=True,
        sampler_process_num=8,
        sampler_timeout=1,
        trainer_timeout=3,
        **kwargs):

    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=epochs
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            DeepWalkSampler(
                name="deepwalk_sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                node_type=node_type,
                node_count=node_count,
                edge_type=edge_type,
                walk_len=walk_len,
                window_size=window_size,
                embedding_dim=embedding_dim,
                neg_num=neg_num,
                temperature=temperature,
                str2hash=str2hash,
                mode="train",
                timeout=sampler_timeout,
                **kwargs
            )
        )
    for p in sampler_processes:
        p.start()
    model_id = _train_graph_model(
        MODEL_CATEGORY["deepwalk"],
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        result_queue,
        trainer_timeout,
        batch_size,
        epochs,
        node_type=node_type,
        node_count=node_count,
        edge_type=edge_type,
        walk_len=walk_len,
        window_size=window_size,
        embedding_dim=embedding_dim,
        neg_num=neg_num,
        temperature=temperature,
        str2hash=str2hash
    )

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return model_id


def train_line(
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        node_type,
        node_count,
        edge_type,
        embedding_dim=64,
        neg_num=5,
        proximity='first_order',
        temperature=1.0,
        str2hash=False,
        batch_size=64,
        epochs=10,
        init_redis=True,
        sampler_process_num=8,
        sampler_timeout=1,
        trainer_timeout=3,
        **kwargs):

    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=epochs
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            LineSampler(
                name="line_sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                node_type=node_type,
                node_count=node_count,
                edge_type=edge_type,
                embedding_dim=embedding_dim,
                neg_num=neg_num,
                temperature=temperature,
                str2hash=str2hash,
                mode="train",
                timeout=sampler_timeout,
                **kwargs
            )
        )
    for p in sampler_processes:
        p.start()

    model_id = _train_graph_model(
        MODEL_CATEGORY["line"],
        nodes_def,
        edges_def,
        redis_host,
        redis_port,
        redis_passwd,
        result_queue,
        trainer_timeout,
        batch_size,
        epochs,
        node_type=node_type,
        node_count=node_count,
        edge_type=edge_type,
        embedding_dim=embedding_dim,
        neg_num=neg_num,
        proximity=proximity,
        temperature=temperature,
        str2hash=str2hash
    )

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return model_id


def evaluate(
        model_id,
        node_type,
        redis_host,
        redis_port,
        redis_passwd=None,
        batch_size=None,
        init_redis=True,
        sampler_process_num=8,
        sampler_timeout=1,
        eval_timeout=3):
    model_info = query_model_info(model_id, "model_category", "model_arch", "save_path")[0]
    model_arch = model_info["model_arch"]
    model_arch["node_type"] = node_type
    nodes_def, edges_def = model_arch.pop("nodes_def"), model_arch.pop("edges_def")

    if not batch_size:
        batch_size = DEFAULT_BATCH_SIZE
    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=1
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            SAMPLER_MAP[model_info["model_category"]](
                name="sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                # node_type=node_type,
                mode="eval",
                timeout=sampler_timeout,
                **model_arch
            )
        )
    for p in sampler_processes:
        p.start()

    model_dir = model_info["save_path"]
    model = CLASS_MAP[model_info["model_category"]](
        graph=build_graph(nodes_def, edges_def, redis_host, redis_port, redis_passwd),
        mode="eval",
        **model_arch
    )
    est = tf.estimator.Estimator(
        model_fn=model.model_fn,
        model_dir=model_dir
    )
    while result_queue.empty():
        time.sleep(0.1)

    metrics = est.evaluate(input_fn=lambda: model.input_fn(lambda: _data_generator(result_queue, eval_timeout)))
    rst = [metrics["loss"]]
    if model_info["model_category"] in (MODEL_CATEGORY["full_gcn"], MODEL_CATEGORY["sampled_gcn"], MODEL_CATEGORY["supervised_sage"]):
        rst.append(metrics["accuracy"])

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return rst


def predict(model_id,
            node_type,
            result_type,
            redis_host,
            redis_port,
            redis_passwd=None,
            result_table=None,
            batch_size=None,
            init_redis=True,
            sampler_process_num=8,
            sampler_timeout=1,
            predict_timeout=3):
    model_info = query_model_info(model_id, "model_category", "model_arch", "save_path")[0]
    predict_col_type = "REAL[]"
    if model_info["model_category"] in (MODEL_CATEGORY["full_gcn"], MODEL_CATEGORY["sampled_gcn"], MODEL_CATEGORY["supervised_sage"]):
        if result_type == "class":
            predict_col_type = "INTEGER"
    model_arch = model_info["model_arch"]
    model_arch["node_type"] = node_type

    if result_table is None:
        result_table = "%s_predict" % model_id
    plpy.execute(
        "CREATE UNLOGGED TABLE %s(id VARCHAR, predict %s) DISTRIBUTED BY (id)" % (result_table, predict_col_type)
    )

    plan = plpy.prepare("INSERT INTO %s VALUES($1, $2)" % result_table, ["VARCHAR", predict_col_type])

    if not batch_size:
        batch_size = DEFAULT_BATCH_SIZE
    nodes_def, edges_def = model_arch.pop("nodes_def"), model_arch.pop("edges_def")
    task_queue = Queue()
    seed_process = _create_seed_process(
        name="seed_process",
        task_queue=task_queue,
        nodes_def=nodes_def,
        edges_def=edges_def,
        node_type=node_type,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_passwd=redis_passwd,
        init_redis=init_redis,
        batch_size=batch_size,
        epochs=1
    )
    seed_process.start()

    result_queue = Queue()
    sampler_processes = []
    for i in range(sampler_process_num):
        sampler_processes.append(
            SAMPLER_MAP[model_info["model_category"]](
                name="sampler_%d" % i,
                task_queue=task_queue,
                result_queue=result_queue,
                nodes_def=nodes_def,
                edges_def=edges_def,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_passwd=redis_passwd,
                # node_type=node_type,
                mode="predict",
                timeout=sampler_timeout,
                **model_arch
            )
        )
    for p in sampler_processes:
        p.start()

    model_dir = model_info["save_path"]
    model = CLASS_MAP[model_info["model_category"]](
        graph=build_graph(nodes_def, edges_def, redis_host, redis_port, redis_passwd),
        mode="predict",
        **model_arch
    )
    est = tf.estimator.Estimator(
        model_fn=model.model_fn,
        model_dir=model_dir,
        params={"predict_mode": result_type}
    )

    for rst in est.predict(
        input_fn=lambda: model.input_fn(lambda: _data_generator(result_queue, predict_timeout))
    ):
        plpy.execute(plan, (rst["id"], rst["predict"].tolist()))

    _join_process(seed_process)
    _join_process(*sampler_processes)

    return result_table


if __name__ == "__main__":
    nodes_def = json.dumps(
        [
            {"node_type": "1", "labeled": True, "label_column": "label", "attr_types": ["int", "string", "int", ], "attr_columns": ["limit", "level", "bill"]},
            {"node_type": "2", "attr_types": ["string", "string"], "attr_columns": ["type", "city"]},
            {"node_type": "3", "attr_types": ["string"], "attr_columns": ["type"]},
        ]
    )
    edges_def = json.dumps(
        [
            {"edge_type": ("1", "1", "1"), "weighted": True, "weight_column": "weight", "directed": False},
            {"edge_type": ("1", "2", "2"), "weighted": True, "weight_column": "weight", "directed": False},
            {"edge_type": ("1", "3", "3"), "weighted": True, "weight_column": "weight", "directed": False},
        ]
    )
    sample_paths = [
        "(1)-[1]->(1)-[1]->(1)-[1]->(1)",
        "(1)-[2]->(2)<-[2]-(1)-[2]->(2)",
        "(1)-[3]->(3)<-[3]-(1)-[3]->(3)"
    ]

    categorical_attrs_desc = {
        "1": {0: ["card_level", 3, 16]},
        "2": {0: ["ip_type", 4, 16], 1: ["ip_city", 10, 16]},
        "3": {0: ["device_type", 3, 16]}
    }

    # train_full_gcn(
    #     nodes_def,
    #     edges_def,
    #     redis_host="192.168.8.138",
    #     redis_port=16379,
    #     redis_passwd=None,
    #     target_node_type="1",
    #     sample_paths=sample_paths,
    #     categorical_attrs_desc=categorical_attrs_desc,
    #     epochs=1
    # )

    # mid = train_sampled_gcn(
    #     nodes_def,
    #     edges_def,
    #     redis_host="192.168.8.138",
    #     redis_port=16379,
    #     redis_passwd=None,
    #     target_node_type="1",
    #     sample_paths=sample_paths,
    #     neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
    #     categorical_attrs_desc=categorical_attrs_desc,
    #     batch_size=64,
    #     epochs=1
    # )

    # mid = train_supervised_sage(
    #     nodes_def,
    #     edges_def,
    #     redis_host="192.168.8.138",
    #     redis_port=16379,
    #     redis_passwd=None,
    #     target_node_type="1",
    #     sample_paths=sample_paths,
    #     neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
    #     categorical_attrs_desc=categorical_attrs_desc,
    #     epochs=1
    # )

    # mid = train_unsupervised_sage(
    #     nodes_def,
    #     edges_def,
    #     redis_host="192.168.8.138",
    #     redis_port=16379,
    #     redis_passwd=None,
    #     target_node_type="1",
    #     sample_paths=sample_paths,
    #     positive_sample_path="(1)-[1]->(1)",
    #     neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
    #     categorical_attrs_desc=categorical_attrs_desc,
    #     epochs=1
    # )

    mid = train_deepwalk(
        json.dumps([{"node_type": "1"}]),
        json.dumps([{"edge_type": ("1", "1", "1"), "weighted": True, "weight_column": "weight", "directed": False}]),
        redis_host="192.168.8.138",
        redis_port=16379,
        redis_passwd=None,
        node_type='1',
        node_count=1000,
        edge_type='1',
        epochs=32
    )

    # mid = train_line(
    #     json.dumps([{"node_type": "1", "weighted": True, "weight_column": "in_degree"}]),
    #     json.dumps([{"edge_type": ("1", "1", "1"), "weighted": True, "weight_column": "weight", "directed": False}]),
    #     redis_host="192.168.8.138",
    #     redis_port=16379,
    #     redis_passwd=None,
    #     node_type='1',
    #     node_count=1000,
    #     edge_type='1',
    #     proximity="second_order",
    #     batch_size=64,
    #     epochs=1024
    # )
    print(mid)

    # m = evaluate(
    #     "model_200011ca2420ee3bbe116d47409f5fd3",
    #     "1",
    #     "192.168.8.138",
    #     16379,
    # )

    # table_name = predict("model_200011ca24201d8abe119d4745da09f0", "1", None, "class", "192.168.8.138", 16379)
    # 
    # print(table_name)
