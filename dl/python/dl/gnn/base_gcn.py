#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

"""class for BaseGCN implementation"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import graphlearn as gl
import tensorflow as tf
import numpy as np
from .utils import ego_types_and_shapes


class BaseGCN(object):
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
                 hidden_act='relu',
                 in_drop_rate=.0,
                 use_input_bn=True,
                 need_dense=True,
                 dense_act=None,
                 mode='train'):
        self.graph = graph
        self._features_num = features_num
        self._node_type = node_type
        self._sample_paths = sample_paths
        self._categorical_attrs_desc = categorical_attrs_desc if categorical_attrs_desc else {}
        self._hidden_dim = hidden_dim
        self._hidden_act = hidden_act
        self._in_drop_rate = in_drop_rate
        self._output_dim = output_dim
        self._neighs_num = neighs_num
        assert mode in ('train', 'eval', 'predict')
        self._mode = mode

        # construct EgoSpecs.
        continuous_attrs_num = {k: features_num[k] - len(self._categorical_attrs_desc.get(k, [])) for k in features_num}
        feature_spec = {
            k: gl.FeatureSpec(
                cont_attrs_num=continuous_attrs_num[k],
                cate_attrs_num=features_num[k]-continuous_attrs_num[k],
                labeled=(self._mode != 'predict') and (k == self._node_type))
            for k in features_num
        }
        hops_spec = []
        pt = re.compile("\((.+?)\)")
        self._hops_num = None
        for path in self._sample_paths:
            node_types_in_path = pt.findall(path)[1:]
            if self._hops_num is None:
                self._hops_num = len(node_types_in_path)
            else:
                assert self._hops_num == len(node_types_in_path), "sample paths should have same length"
            for node_t in node_types_in_path:
                hops_spec.append(gl.HopSpec(feature_spec[node_t], sparse=self._neighs_num is None))
        self._src_ego_spec = gl.EgoSpec(feature_spec[self._node_type], hops_spec=hops_spec)

        if self._neighs_num is not None:
            for num in self._neighs_num:
                assert len(num) == self._hops_num

        # encoders.
        self._use_input_bn = use_input_bn
        self._need_dense = need_dense
        self._dense_act = dense_act

        # fc layer at end
        if self._output_dim:
            self._fc = tf.layers.Dense(units=self._output_dim)

    def receptive_fn(self, t, node_ids):
        raise NotImplementedError("`receptive_fn` not implemented yet")

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

        encodes = []
        pt = re.compile("\((.+?)\)")
        for idx, path in enumerate(self._sample_paths):
            feat_encoders = [feature_encoders[n_type] for n_type in pt.findall(path)]
            conv_layers = []
            for i in range(depth):
                conv_layers.append(gl.layers.GCNConv(self._hidden_dim, self._hidden_act))
            if self._neighs_num is None:
                encoder = gl.encoders.SparseEgoGraphEncoder(
                    feat_encoders,
                    conv_layers,
                    dropout=self._in_drop_rate
                )
            else:
                encoder = gl.encoders.EgoGraphEncoder(feat_encoders,
                                                      conv_layers,
                                                      nbr_num_list=self._neighs_num[idx],
                                                      dropout=self._in_drop_rate)
            encodes.append(encoder)

        return {"src": encodes, "edge": None, "dst": None}

    def _accuracy(self, logits, labels):
        """Accuracy for supervised model.
        Args:
          logits: embeddings, 2D tensor with shape [batchsize, dimension]
          labels: 1D tensor with shape [batchsize]
        """
        indices = tf.math.argmax(logits, 1, output_type=tf.int32)
        # correct = tf.reduce_sum(tf.cast(tf.math.equal(indices, labels), tf.float32))
        return tf.metrics.accuracy(labels, indices)

    def _supervised_loss(self, emb, label):
        return gl.softmax_cross_entropy_loss(emb, label)

    def model_fn(self, features, labels, mode, params, config):
        if self._mode == "predict":
            ids = features[1]
            features = gl.EgoTensor(features[0], self._src_ego_spec)
        else:
            features = gl.EgoTensor(features, self._src_ego_spec)
        encoders = self._encoders()["src"]
        src_emb = tf.reduce_sum(
            [encoders[i].encode(features, start=i * self._hops_num) for i in
             range(self._sample_paths.__len__())],
            axis=0
        )
        logits = self._fc(src_emb)
        # for predict
        if mode == tf.estimator.ModeKeys.PREDICT:
            assert self._mode == "predict", "`mode` expect `predict`, but got `%s`" % self._mode
            predict_mode = params.get("predict_mode", 'class')
            if predict_mode == "embedding":
                predictions = src_emb
            elif predict_mode == "logit":
                predictions = logits
            else:
                predictions = tf.argmax(logits, axis=1)
            return tf.estimator.EstimatorSpec(
                mode=mode,
                predictions={"id": ids, "predict": predictions}
            )
        loss = self._supervised_loss(logits, labels)
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
                eval_metric_ops={"accuracy": self._accuracy(logits, labels)}
            )

        raise ValueError("Invalid `mode` value: %s" % mode)

    def get_output_types_shapes(self):
        output_types, output_shapes = ego_types_and_shapes(self._src_ego_spec)
        if self._mode == "predict":
            output_types, output_shapes = (output_types, tf.int64), (output_shapes, tf.TensorShape([None]))
        return output_types, output_shapes

    def input_fn(self, date_generator):
        output_types, output_shapes = self.get_output_types_shapes()
        return tf.data.Dataset.from_generator(
            date_generator,
            output_types=(output_types, tf.int32),
            output_shapes=(output_shapes, tf.TensorShape([None]))
        )

    def _sample_generator(self, sample_seed):
        """Sample using sample functions and return a wrapped generator

        Returns:
          Tuple of egoGraphs
        """
        while True:
            try:
                batch_seed = sample_seed().next()
                src_recept = self.receptive_fn(batch_seed.type, batch_seed.ids).flatten(spec=self._src_ego_spec)
                if self._mode == "predict":
                    yield (tuple(src_recept), batch_seed.ids), batch_seed.ids.astype(np.int32)
                else:
                    yield tuple(src_recept), batch_seed.labels
            except gl.errors.OutOfRangeError:
                break

    @property
    def src_ego_spec(self):
        return self._src_ego_spec

    @property
    def mode(self):
        return self._mode


if __name__ == "__main__":
    pass
