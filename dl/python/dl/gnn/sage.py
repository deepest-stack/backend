#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

"""class for SAGE implementation"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import graphlearn as gl
import tensorflow as tf
import numpy as np
from .sampled_gcn import SampledGCN
from .utils import edge_ori, ego_types_and_shapes


class SupervisedSAGE(SampledGCN):
    """
    Args:
      graph: Initialized gl.Graph object.
      output_dim: Output dimension.
      features_num: dict, format as {node_type: features_num}
      node_type: target node type
      sample_paths: list of sample paths, format as ["(node_type)-[edge_type]->(node_type)", "(node_type)<-[edge_type]-(node_type)"].
      neighs_num: A list indicates number of neighbors to sample in each hop for each path,
                [[path1-hop1, path1-hop2, path1-hop3], [path2-hop1, path2-hop2, path2-hop3], [path3-hop1, path3-hop2, path3-hop3]]
      agg_type: neighbour aggregate function type
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
                 agg_type="gcn",
                 categorical_attrs_desc=None,
                 hidden_dim=16,
                 hidden_act='relu',
                 in_drop_rate=.0,
                 use_input_bn=True,
                 need_dense=True,
                 dense_act=None,
                 mode="train"):
        super(SupervisedSAGE, self).__init__(
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
        self._agg_type = agg_type

    def _encoders(self):
        """
        return a dict of encode
        """

        depth = self._hops_num

        feature_encoders = {}
        for k in self._features_num:
            feature_encoders[k] = gl.encoders.WideNDeepEncoder(
                self._categorical_attrs_desc[k],
                self._features_num[k],
                self._hidden_dim,
                use_input_bn=self._use_input_bn,
                need_dense=self._need_dense,
                act=self._dense_act,
                name=k+"_feat_encoder"
            )

        src_encodes = []
        pt = re.compile("\((.+?)\)")
        for idx, path in enumerate(self._sample_paths):
            feat_encoders = [feature_encoders[n_type] for n_type in pt.findall(path)]
            conv_layers = []
            for i in range(depth):
                conv_layers.append(
                    gl.layers.GraphSageConv(
                        index=i,
                        in_dim=self._hidden_dim,
                        out_dim=self._hidden_dim,
                        agg_type=self._agg_type,
                        act=self._hidden_act
                    )
                )
            encoder = gl.encoders.EgoGraphEncoder(
                feat_encoders,
                conv_layers,
                nbr_num_list=self._neighs_num[idx],
                dropout=self._in_drop_rate)
            src_encodes.append(encoder)

        return {"src": src_encodes, "edge": None, "dst": None}


class UnsupervisedSAGE(SupervisedSAGE):
    """
    Args:
      graph: Initialized gl.Graph object.
      features_num: dict, format as {node_type: features_num}
      node_type: target node type
      sample_paths: list of sample paths, format as
        ["(node_type)-[edge_type]->(node_type)",
        "(node_type)<-[edge_type]-(node_type)"].
      positive_sample_path: positive sample path, the type of head & tail node in path must be `node_type`,
        and the tail node will be picked as positive sample
      neighs_num: A list indicates number of neighbors to sample in each hop for each path,
        [[path1-hop1, path1-hop2, path1-hop3],
        [path2-hop1, path2-hop2, path2-hop3],
        [path3-hop1, path3-hop2, path3-hop3]]
      neg_num: negative sample number
      agg_type: neighbour aggregate function type
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
                 mode="train"):
        super(UnsupervisedSAGE, self).__init__(
            graph=graph,
            output_dim=None,
            features_num=features_num,
            node_type=node_type,
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
            mode=mode
        )
        self._neg_num = neg_num
        self._positive_sample_path = positive_sample_path

    def positive_sample(self, ids):
        pt = re.compile("(<?-)\[(.+?)\](->?)")
        e_type_ori = [(group[1], edge_ori(group)) for group in pt.findall(self._positive_sample_path)]
        sample_func = lambda v, params: \
            v.outV(params[0]).sample(1).by("random") if params[1] == 1 \
                else v.inV(params[0]).sample(1).by('random')
        return self.graph.V(self._node_type, feed=ids).repeat(
            sample_func,
            times=len(e_type_ori),
            params_list=e_type_ori

        ).emit(lambda x: x[-1])

    def negative_sample(self, ids):
        return self.graph.V(
            self._node_type,
            feed=ids
        ).outNeg(self._node_type).sample(self._neg_num).by('random').emit(lambda x: x[-1])

    def _unsupervised_loss(self, src_emb, pos_dst_emb, neg_dst_emb):
        return gl.sigmoid_cross_entropy_loss(src_emb, pos_dst_emb, neg_dst_emb, 'dot')

    def _sample_generator(self, sample_seed):
        while True:
            try:
                batch_seed = sample_seed().next()
                src_recept = self.receptive_fn(batch_seed.type, batch_seed.type).flatten(spec=self._src_ego_spec)
                if self._mode == "predict":
                    yield (tuple(src_recept), batch_seed.ids), batch_seed.ids.astype(np.int32)
                else:
                    p_nodes = self.positive_sample(batch_seed.ids)
                    pos_recept = self.receptive_fn(p_nodes.type, p_nodes.ids).flatten(spec=self._src_ego_spec)
                    n_nodes = self.negative_sample(batch_seed.ids)
                    neg_recept = self.receptive_fn(n_nodes.type, n_nodes.ids).flatten(spec=self._src_ego_spec)
                    yield (tuple(src_recept), tuple(pos_recept), tuple(neg_recept)), batch_seed.ids.astype(np.int32)
            except gl.errors.OutOfRangeError:
                break

    def get_output_types_shapes(self):
        output_types, output_shapes = ego_types_and_shapes(self._src_ego_spec)
        if self._mode != "predict":
            output_types = (output_types, output_types, output_types)
            output_shapes = (output_shapes, output_shapes, output_shapes)
        else:
            output_types, output_shapes = (output_types, tf.int64), (output_shapes, tf.TensorShape([None]))
        return output_types, output_shapes

    def model_fn(self, features, labels, mode, params, config):
        encoders = self._encoders()["src"]

        src_emb = tf.reduce_sum(
            [encoders[i].encode(gl.EgoTensor(features[0], self._src_ego_spec), start=i * self._hops_num) for i in
             range(self._sample_paths.__len__())],
            axis=0
        )
        # for predict
        if mode == tf.estimator.ModeKeys.PREDICT:
            return tf.estimator.EstimatorSpec(
                mode=mode,
                predictions={"id": features[1], "predict": src_emb}
            )
        pos_emb = tf.reduce_sum(
            [encoders[i].encode(gl.EgoTensor(features[1], self._src_ego_spec), start=i * self._hops_num) for i in
             range(self._sample_paths.__len__())],
            axis=0
        )
        neg_emb = tf.reduce_sum(
            [encoders[i].encode(gl.EgoTensor(features[2], self._src_ego_spec), start=i * self._hops_num) for i in
             range(self._sample_paths.__len__())],
            axis=0
        )
        loss = self._unsupervised_loss(src_emb, pos_emb, neg_emb)[0]
        # for train
        if mode == tf.estimator.ModeKeys.TRAIN:
            train_op = params.get(
                "optimizer",
                tf.train.AdamOptimizer()
            ).minimize(
                loss,
                global_step=tf.train.get_or_create_global_step()
            )
            return tf.estimator.EstimatorSpec(
                mode=mode,
                loss=loss,
                train_op=train_op
            )
        # for eval
        if mode == tf.estimator.ModeKeys.EVAL:
            return tf.estimator.EstimatorSpec(
                mode=mode,
                loss=loss,
            )


def train_supervised():
    global g, sample_paths, categorical_attrs_desc
    sage = SupervisedSAGE(
        graph=g,
        output_dim=2,
        features_num={"1": 3, "2": 2, "3": 1},
        node_type="1",
        sample_paths=sample_paths,
        neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
        agg_type="gcn",
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=16,
        hidden_act=tf.nn.relu,
        in_drop_rate=0.1,
        use_input_bn=True,
        need_dense=True,
        dense_act=None
    )
    est = tf.estimator.Estimator(
        model_fn=sage.model_fn,
        model_dir="/gpload/sage_model_dir1"
    )
    epochs = 1
    train_sample_seed = lambda: sage.graph.V("1").shuffle(traverse=True).batch(32).values()
    est.train(
        input_fn=lambda: sage.input_fn(sample_seed=train_sample_seed, epochs=epochs)
    )


def train_unsupervised():
    global g, sample_paths, categorical_attrs_desc
    sage = UnsupervisedSAGE(
        graph=g,
        features_num={"1": 3, "2": 2, "3": 1},
        node_type="1",
        sample_paths=sample_paths,
        positive_sample_path="(1)-[1]->(1)",
        neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
        neg_num=5,
        agg_type="gcn",
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=16,
        hidden_act=tf.nn.relu,
        in_drop_rate=0.1,
        use_input_bn=True,
        need_dense=True,
        dense_act=None
    )
    est = tf.estimator.Estimator(
        model_fn=sage.model_fn,
        model_dir="/gpload/unsage_model_dir2"
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
    sage = UnsupervisedSAGE(
        graph=g,
        features_num={"1": 3, "2": 2, "3": 1},
        node_type="1",
        sample_paths=sample_paths,
        positive_sample_path="(1)-[1]->(1)",
        neighs_num=[[4, 4, 4], [2, 2, 2], [3, 3, 3]],
        neg_num=5,
        agg_type="gcn",
        categorical_attrs_desc=categorical_attrs_desc,
        hidden_dim=16,
        hidden_act=tf.nn.relu,
        in_drop_rate=0.1,
        use_input_bn=True,
        need_dense=True,
        dense_act=None,
        mode="predict"
    )
    est = tf.estimator.Estimator(
        model_fn=sage.model_fn,
        model_dir="/gpload/unsage_model_dir2"
    )
    predict_sample_seed = lambda: sage.graph.V("1").shuffle(traverse=True).batch(32).values()
    for emb in est.predict(
        input_fn=lambda: sage.input_fn(sample_seed=predict_sample_seed, epochs=1)
    ):
        print(emb)


if __name__ == "__main__":
    from .utils import load_graph
    g = load_graph()
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
    # train_supervised()
    train_unsupervised()
    # predict()
