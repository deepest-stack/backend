#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

"""class for sampled GCN implementation"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import graphlearn as gl
import tensorflow as tf
from .base_gcn import BaseGCN
from .utils import edge_ori


class SampledGCN(BaseGCN):
    """
    Args:
      graph: Initialized gl.Graph object.
      output_dim: Output dimension.
      features_num: dict, format as {node_type: features_num}
      node_type: target node type
      sample_paths: list of sample paths, format as ["(node_type)-[edge_type]->(node_type)", "(node_type)<-[edge_type]-(node_type)"].
      neighs_num: A list indicates number of neighbors to sample in each hop for each path,
                [[path1-hop1, path1-hop2, path1-hop3], [path2-hop1, path2-hop2, path2-hop3], [path3-hop1, path3-hop2, path3-hop3]]
      categorical_attrs_desc: A dict indicates discrete features, with the format
      {node_type: {feature_column_index : [name, discrete_features_count, embedding_dimension]}}.
      hidden_dim: Hidden dimension.
      hidden_act: action function for hidden layers
      in_drop_rate: Dropout ratio for input data.
      need_dense: whether use dense layer for feature encoder
      dense_act: action function of dense layer in feature encoder
    """

    def __init__(self,
                 graph,
                 output_dim,
                 features_num,
                 node_type,
                 sample_paths,
                 neighs_num,
                 categorical_attrs_desc=None,
                 hidden_dim=16,
                 hidden_act="relu",
                 in_drop_rate=.0,
                 use_input_bn=True,
                 need_dense=True,
                 dense_act=None,
                 mode="train"):
        super(SampledGCN, self).__init__(
            graph=graph,
            output_dim=output_dim,
            features_num=features_num,
            node_type=node_type,
            sample_paths=sample_paths,
            neighs_num=neighs_num,
            categorical_attrs_desc=categorical_attrs_desc,
            hidden_dim=hidden_dim,
            hidden_act=hidden_act,
            in_drop_rate=in_drop_rate,
            use_input_bn=use_input_bn,
            need_dense=need_dense,
            dense_act=dense_act,
            mode=mode
        )

    def receptive_fn(self, t, node_ids):
        alias_list = [['v_%d_%d' % (j+1, i+1) for i in range(self._hops_num)] for j in range(len(self._sample_paths))]
        pt = re.compile("(<?-)\[(.+?)\](->?)")
        e_type_ori = [[(group[1], edge_ori(group)) for group in pt.findall(sample_path)] for sample_path in self._sample_paths]
        params_list = [zip(e_type_ori[idx], self._neighs_num[idx]) for idx in range(len(self._sample_paths))]

        sample_func = lambda v, params: \
            v.outV(params[0][0]).sample(params[1]).by('topk') if params[0][1] == 1 \
                else v.inV(params[0][0]).sample(params[1]).by('topk')

        src, layers = self.graph.V(t, feed=node_ids).alias('v').each(
            lambda v: [
                v.repeat(
                    sample_func,
                    self._hops_num,
                    params_list=params_list[idx],
                    alias_list=alias_list[idx]
                ) for idx in range(len(self._sample_paths))]
        ).emit(lambda x: (x["v"], [gl.Layer(nodes=x[name]) for alias in alias_list for name in alias]))

        return gl.EgoGraph(src, layers)


def train():
    global g, sample_paths, categorical_attrs_desc
    sage = SampledGCN(
        graph=g,
        output_dim=2,
        features_num={"1": 3, "2": 2, "3": 1},
        node_type="1",
        sample_paths=sample_paths,
        neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=16,
        hidden_act='relu',
        in_drop_rate=0.1,
        use_input_bn=True,
        need_dense=True,
        dense_act=None
    )
    est = tf.estimator.Estimator(
        model_fn=sage.model_fn,
        model_dir="/gpload/model_dir/sampled_gcn",
    )

    epochs = 1
    train_sample_seed = lambda: sage.graph.V("1").shuffle(traverse=True).batch(64).values()
    est.train(
        input_fn=lambda: sage.input_fn(sample_seed=train_sample_seed, epochs=epochs)
    )


def predict():
    # *** NOTE ***
    # the node to predict should be non-labeled
    # i.e. `gl.Decoder(labeled=False, ...)`
    global g, sample_paths, categorical_attrs_desc
    sage = SampledGCN(
        graph=g,
        output_dim=2,
        features_num={"1": 3, "2": 2, "3": 1},
        node_type="1",
        sample_paths=sample_paths,
        neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=16,
        hidden_act='relu',
        in_drop_rate=0.1,
        use_input_bn=True,
        need_dense=True,
        dense_act=None,
        mode="predict"
    )
    est = tf.estimator.Estimator(
        model_fn=sage.model_fn,
        model_dir="/gpload/model_dir/sampled_gcn",
        params={"predict_mode": "class"}
    )

    predict_sample_seed = lambda: sage.graph.V("1").shuffle(traverse=True).batch(32).values()
    for emb in est.predict(
        input_fn=lambda: sage.input_fn(sample_seed=predict_sample_seed, epochs=1)
    ):
        print(emb)


def evaluate():
    global g, sample_paths, categorical_attrs_desc
    sage = SampledGCN(
        graph=g,
        output_dim=2,
        features_num={"1": 3, "2": 2, "3": 1},
        node_type="1",
        sample_paths=sample_paths,
        neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=16,
        hidden_act='relu',
        in_drop_rate=0.1,
        use_input_bn=True,
        need_dense=True,
        dense_act=None,
        mode="eval"
    )
    est = tf.estimator.Estimator(
        model_fn=sage.model_fn,
        model_dir="/gpload/model_dir/sampled_gcn",
    )

    eval_sample_seed = lambda: sage.graph.V("1").shuffle(traverse=True).batch(32).values()
    print(est.evaluate(input_fn=lambda: sage.input_fn(sample_seed=eval_sample_seed, epochs=1)))


if __name__ == "__main__":
    from .utils import load_graph
    g = load_graph(True)
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
    train()
    # evaluate()
