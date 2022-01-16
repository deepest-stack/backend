#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8


from .gnn import FullGCN, SupervisedSAGE, SampledGCN, UnsupervisedSAGE
from .graph_embedding import DeepWalk, LINE
from multiprocessing import Process
from multiprocessing import Queue
from .utils import build_graph
import numpy as np


class SeedProcess(Process):
    def __init__(self,
                 name,
                 task_queue,
                 nodes_def,
                 edges_def,
                 node_type,
                 redis_host,
                 redis_port,
                 redis_passwd=None,
                 batch_size=32,
                 epochs=1,
                 **kwargs):
        super(SeedProcess, self).__init__(name=name)
        self._task_queue = task_queue
        self._node_type = node_type
        self._batch_size = batch_size
        self._epochs = epochs
        self._graph = build_graph(nodes_def, edges_def, redis_host, redis_port, redis_passwd, **kwargs)

    def run(self):
        gen = self._graph.V(self._node_type).shuffle(traverse=True).batch(self._batch_size).values()
        current_epoch = 0
        while True:
            try:
                seed = gen.next()
                ids, labels = seed.ids, seed.labels
                if labels is None:
                    labels = ids
                self._task_queue.put((ids, labels))
            except:
                if current_epoch < self._epochs:
                    current_epoch += 1
                else:
                    break


class SamplerProcess(Process):
    def __init__(self,
                 name,
                 task_queue,
                 result_queue,
                 nodes_def,
                 edges_def,
                 node_type,
                 redis_host,
                 redis_port,
                 redis_passwd=None,
                 timeout=1,
                 **kwargs):
        super(SamplerProcess, self).__init__(name=name)
        self._task_queue = task_queue
        self._result_queue = result_queue
        self._timeout = timeout
        self._graph = build_graph(nodes_def, edges_def, redis_host, redis_port, redis_passwd, **kwargs)
        self._node_type = node_type

    def _build_model(self):
        raise NotImplementedError("model build method function `_build_model` not implemented yet")

    def run(self):
        model = self._build_model()
        while True:
            try:
                ids, labels = self._task_queue.get(timeout=self._timeout)
            except Exception as e:
                break
            src_recept = model.receptive_fn(self._node_type, ids).flatten(spec=model.src_ego_spec)
            if model.mode == "predict":
                self._result_queue.put(((tuple(src_recept), ids), ids.astype(np.int32)))
            else:
                self._result_queue.put((tuple(src_recept), labels))


class FullGCNSampler(SamplerProcess):
    def __init__(self,
                 name,
                 task_queue,
                 result_queue,
                 nodes_def,
                 edges_def,
                 redis_host,
                 redis_port,
                 redis_passwd,
                 output_dim,
                 features_num,
                 node_type,
                 sample_paths,
                 categorical_attrs_desc=None,
                 hidden_dim=16,
                 hidden_act='relu',
                 in_drop_rate=.0,
                 use_input_bn=True,
                 need_dense=True,
                 dense_act=None,
                 mode="train",
                 timeout=1,
                 **kwargs
                 ):
        super(FullGCNSampler, self).__init__(
            name,
            task_queue,
            result_queue,
            nodes_def,
            edges_def,
            node_type,
            redis_host,
            redis_port,
            redis_passwd,
            timeout,
            **kwargs
        )
        self._nodes_def = nodes_def
        self._edges_def = edges_def
        self._output_dim = output_dim
        self._features_num = features_num
        self._node_type = node_type
        self._sample_paths = sample_paths
        self._categorical_attrs_desc = categorical_attrs_desc
        self._hidden_dim = hidden_dim
        self._hidden_act = hidden_act
        self._in_drop_rate = in_drop_rate
        self._use_input_bn = use_input_bn
        self._need_dense = need_dense
        self._dense_act = dense_act
        self._mode = mode

    def _build_model(self):
        return FullGCN(
            graph=self._graph,
            output_dim=self._output_dim,
            features_num=self._features_num,
            node_type=self._node_type,
            sample_paths=self._sample_paths,
            categorical_attrs_desc=self._categorical_attrs_desc,
            hidden_dim=self._hidden_dim,
            hidden_act=self._hidden_act,
            in_drop_rate=self._in_drop_rate,
            use_input_bn=self._use_input_bn,
            need_dense=self._need_dense,
            dense_act=self._dense_act,
            mode=self._mode
        )


class SampledGCNSampler(SamplerProcess):
    def __init__(self,
                 name,
                 task_queue,
                 result_queue,
                 nodes_def,
                 edges_def,
                 redis_host,
                 redis_port,
                 redis_passwd,
                 output_dim,
                 features_num,
                 node_type,
                 sample_paths,
                 neighs_num,
                 categorical_attrs_desc=None,
                 hidden_dim=16,
                 hidden_act='relu',
                 in_drop_rate=.0,
                 use_input_bn=True,
                 need_dense=True,
                 dense_act=None,
                 mode="train",
                 timeout=1,
                 **kwargs):
        super(SampledGCNSampler, self).__init__(
            name,
            task_queue,
            result_queue,
            nodes_def,
            edges_def,
            node_type,
            redis_host,
            redis_port,
            redis_passwd,
            timeout,
            **kwargs
        )
        self._nodes_def = nodes_def
        self._edges_def = edges_def
        self._output_dim = output_dim
        self._features_num = features_num
        self._node_type = node_type
        self._sample_paths = sample_paths
        self._neighs_num = neighs_num
        self._categorical_attrs_desc = categorical_attrs_desc
        self._hidden_dim = hidden_dim
        self._hidden_act = hidden_act
        self._in_drop_rate = in_drop_rate
        self._use_input_bn = use_input_bn
        self._need_dense = need_dense
        self._dense_act = dense_act
        self._mode = mode

    def _build_model(self):
        return SampledGCN(
            graph=self._graph,
            output_dim=self._output_dim,
            features_num=self._features_num,
            node_type=self._node_type,
            sample_paths=self._sample_paths,
            neighs_num=self._neighs_num,
            categorical_attrs_desc=self._categorical_attrs_desc,
            hidden_dim=self._hidden_dim,
            hidden_act=self._hidden_act,
            in_drop_rate=self._in_drop_rate,
            use_input_bn=self._use_input_bn,
            need_dense=self._need_dense,
            dense_act=self._dense_act,
            mode=self._mode
        )


class SupervisedSAGESampler(SamplerProcess):
    def __init__(self,
                 name,
                 task_queue,
                 result_queue,
                 nodes_def,
                 edges_def,
                 redis_host,
                 redis_port,
                 redis_passwd,
                 output_dim,
                 features_num,
                 node_type,
                 sample_paths,
                 neighs_num,
                 agg_type="gcn",
                 categorical_attrs_desc=None,
                 hidden_dim=16,
                 hidden_act='relu',
                 in_drop_rate=.0,
                 use_input_bn=True,
                 need_dense=True,
                 dense_act=None,
                 mode="train",
                 timeout=1,
                 **kwargs
                 ):
        super(SupervisedSAGESampler, self).__init__(
            name,
            task_queue,
            result_queue,
            nodes_def,
            edges_def,
            node_type,
            redis_host,
            redis_port,
            redis_passwd,
            timeout,
            **kwargs
        )
        self._nodes_def = nodes_def
        self._edges_def = edges_def
        self._output_dim = output_dim
        self._features_num = features_num
        self._node_type = node_type
        self._sample_paths = sample_paths
        self._neighs_num = neighs_num
        self._agg_type = agg_type
        self._categorical_attrs_desc = categorical_attrs_desc
        self._hidden_dim = hidden_dim
        self._hidden_act = hidden_act
        self._in_drop_rate = in_drop_rate
        self._use_input_bn = use_input_bn
        self._need_dense = need_dense
        self._dense_act = dense_act
        self._mode = mode

    def _build_model(self):
        return SupervisedSAGE(
            graph=self._graph,
            output_dim=self._output_dim,
            features_num=self._features_num,
            node_type=self._node_type,
            sample_paths=self._sample_paths,
            neighs_num=self._neighs_num,
            agg_type=self._agg_type,
            categorical_attrs_desc=self._categorical_attrs_desc,
            hidden_dim=self._hidden_dim,
            hidden_act=self._hidden_act,
            in_drop_rate=self._in_drop_rate,
            use_input_bn=self._use_input_bn,
            need_dense=self._need_dense,
            dense_act=self._dense_act,
            mode=self._mode
        )


class UnsupervisedSAGESampler(SamplerProcess):
    def __init__(self,
                 name,
                 task_queue,
                 result_queue,
                 nodes_def,
                 edges_def,
                 redis_host,
                 redis_port,
                 redis_passwd,
                 features_num,
                 node_type,
                 sample_paths,
                 positive_sample_path,
                 neighs_num,
                 neg_num=10,
                 agg_type="gcn",
                 categorical_attrs_desc=None,
                 hidden_dim=16,
                 hidden_act='relu',
                 in_drop_rate=.0,
                 use_input_bn=True,
                 need_dense=True,
                 dense_act=None,
                 mode="train",
                 timeout=1,
                 **kwargs
                 ):
        super(UnsupervisedSAGESampler, self).__init__(
            name,
            task_queue,
            result_queue,
            nodes_def,
            edges_def,
            node_type,
            redis_host,
            redis_port,
            redis_passwd,
            timeout,
            **kwargs
        )
        self._node_def = nodes_def
        self._edge_def = edges_def
        self._features_num = features_num
        self._node_type = node_type
        self._sample_paths = sample_paths
        self._positive_sample_path = positive_sample_path
        self._neighs_num = neighs_num
        self._neg_num = neg_num
        self._agg_type = agg_type
        self._categorical_attrs_desc = categorical_attrs_desc
        self._hidden_dim = hidden_dim
        self._hidden_act = hidden_act
        self._in_drop_rate = in_drop_rate
        self._use_input_bn = use_input_bn
        self._need_dense = need_dense
        self._dense_act = dense_act
        self._mode = mode

    def _build_model(self):
        return UnsupervisedSAGE(
            graph=self._graph,
            features_num=self._features_num,
            node_type=self._node_type,
            sample_paths=self._sample_paths,
            positive_sample_path=self._positive_sample_path,
            neighs_num=self._neighs_num,
            neg_num=self._neg_num,
            agg_type=self._agg_type,
            categorical_attrs_desc=self._categorical_attrs_desc,
            hidden_dim=self._hidden_dim,
            hidden_act=self._hidden_act,
            in_drop_rate=self._in_drop_rate,
            use_input_bn=self._use_input_bn,
            need_dense=self._need_dense,
            dense_act=self._dense_act,
            mode=self._mode
        )

    def run(self):
        model = self._build_model()
        while True:
            try:
                ids, _ = self._task_queue.get(timeout=self._timeout)
            except Exception as e:
                break
            src_recept = model.receptive_fn(self._node_type, ids).flatten(spec=model.src_ego_spec)
            if model.mode == "predict":
                self._result_queue.put(((tuple(src_recept), ids), ids.astype(np.int32)))
            else:
                p_nodes = model.positive_sample(ids)
                pos_recept = model.receptive_fn(p_nodes.type, p_nodes.ids).flatten(spec=model.src_ego_spec)
                n_nodes = model.negative_sample(ids)
                neg_recept = model.receptive_fn(n_nodes.type, n_nodes.ids).flatten(spec=model.src_ego_spec)
                self._result_queue.put(((tuple(src_recept), tuple(pos_recept), tuple(neg_recept)), ids.astype(np.int32)))


class LineSampler(SamplerProcess):

    def __init__(self,
                 name,
                 task_queue,
                 result_queue,
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
                 mode="train",
                 timeout=1,
                 **kwargs
                 ):
        super(LineSampler, self).__init__(
            name,
            task_queue,
            result_queue,
            nodes_def,
            edges_def,
            node_type,
            redis_host,
            redis_port,
            redis_passwd,
            timeout,
            **kwargs
        )
        self._node_type = node_type
        self._node_count = node_count
        self._edge_type = edge_type
        self._embedding_dim = embedding_dim
        self._neg_num = neg_num
        self._proximity = proximity
        self._temperature = temperature
        self._str2hash = str2hash
        self._mode = mode

    def _build_model(self):
        return LINE(
            graph=self._graph,
            node_type=self._node_type,
            node_count=self._node_count,
            edge_type=self._edge_type,
            embedding_dim=self._embedding_dim,
            neg_num=self._neg_num,
            proximity=self._proximity,
            temperature=self._temperature,
            str2hash=self._str2hash,
            mode=self._mode
        )

    def run(self):
        model = self._build_model()
        while True:
            try:
                ids, _ = self._task_queue.get(timeout=self._timeout)
            except Exception as e:
                break
            if self._mode == "predict":
                self._result_queue.put((ids, ids.astype(np.int32)))
            else:
                if self._proximity == "second_order":
                    pos_dst_ids = model.positive_sample(ids)
                else:
                    pos_dst_ids = ids
                neg_dst_ids = model.negative_sample(ids)
                self._result_queue.put(((ids, pos_dst_ids, neg_dst_ids), ids.astype(np.int32)))


class DeepWalkSampler(SamplerProcess):

    def __init__(self,
                 name,
                 task_queue,
                 result_queue,
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
                 mode="train",
                 timeout=1,
                 **kwargs
                 ):
        super(DeepWalkSampler, self).__init__(
            name,
            task_queue,
            result_queue,
            nodes_def,
            edges_def,
            node_type,
            redis_host,
            redis_port,
            redis_passwd,
            timeout,
            **kwargs
        )
        self._node_type = node_type
        self._node_count = node_count
        self._edge_type = edge_type
        self._embedding_dim = embedding_dim
        self._neg_num = neg_num
        self._walk_len = walk_len
        self._window_size = window_size
        self._temperature = temperature
        self._str2hash = str2hash
        self._mode = mode

    def _build_model(self):
        return DeepWalk(
            graph=self._graph,
            node_type=self._node_type,
            node_count=self._node_count,
            edge_type=self._edge_type,
            embedding_dim=self._embedding_dim,
            neg_num=self._neg_num,
            walk_len=self._walk_len,
            window_size=self._window_size,
            temperature=self._temperature,
            str2hash=self._str2hash,
            mode=self._mode
        )

    def run(self):
        model = self._build_model()
        while True:
            try:
                ids, _ = self._task_queue.get(timeout=self._timeout)
            except Exception as e:
                break
            if self._mode == "predict":
                self._result_queue.put((ids, ids.astype(np.int32)))
            else:
                src_ids, dst_ids = model.positive_sample(ids)
                self._result_queue.put(((src_ids, dst_ids), ids.astype(np.int32)))


class Consumer(Process):
    def __init__(self, name, q):
        super(Consumer, self).__init__(name=name)
        self._q = q

    def run(self):
        while True:
            i = self._q.get(timeout=10)
            print("process: %s, i: %s" % (self.name, i))


if __name__ == "__main__":
    import json, time
    redis_host_ = "192.168.8.138"
    redis_port_ = 16379

    nodes_def_ = json.dumps(
        [
            {"node_type": "1", "labeled": True, "label_column": "label", "attr_types": ["int", "string", "int", ],
             "attr_columns": ["limit", "level", "bill"]},
            {"node_type": "2", "attr_types": ["string", "string"], "attr_columns": ["type", "city"]},
            {"node_type": "3", "attr_types": ["string"], "attr_columns": ["type"]},
        ]
    )
    edges_def_ = json.dumps(
        [
            {"edge_type": ("1", "1", "1"), "weighted": True, "weight_column": "weight", "directed": False},
            {"edge_type": ("1", "2", "2"), "weighted": True, "weight_column": "weight", "directed": False},
            {"edge_type": ("1", "3", "3"), "weighted": True, "weight_column": "weight", "directed": False},
        ]
    )
    sample_paths_ = [
        "(1)-[1]->(1)-[1]->(1)-[1]->(1)",
        "(1)-[2]->(2)<-[2]-(1)-[2]->(2)",
        "(1)-[3]->(3)<-[3]-(1)-[3]->(3)"
    ]

    categorical_attrs_desc_ = {
        "1": {0: ["card_level", 3, 16]},
        "2": {0: ["ip_type", 4, 16], 1: ["ip_city", 10, 16]},
        "3": {0: ["device_type", 3, 16]}
    }
    task_queue_ = Queue()
    result_queue_ = Queue()
    seed_p = SeedProcess(
        name="seed_process",
        task_queue=task_queue_,
        nodes_def=nodes_def_,
        edges_def=edges_def_,
        node_type="1",
        redis_host=redis_host_,
        redis_port=redis_port_,
        init_redis=True,
        epochs=1
    )
    seed_p.start()

    p_list = []
    for i in range(10):
        p = FullGCNSampler(
            name="sampler_%d" % i,
            task_queue=task_queue_,
            result_queue=result_queue_,
            nodes_def=nodes_def_,
            edges_def=edges_def_,
            redis_host=redis_host_,
            redis_port=redis_port_,
            redis_passwd=None,
            features_num={"1": 3, "2": 2, "3": 1},
            output_dim=None,
            node_type="1",
            sample_paths=sample_paths_,
        )
        p_list.append(p)

    for p in p_list:
        p.start()

    while result_queue_.empty():
        print("result queue is empty ......")
        time.sleep(0.1)

    p1 = Consumer("consumer", result_queue_)

    p1.start()

    seed_p.join()
    for p in p_list:
        p.join()
    p1.join()
    print("ZZZZZ")



