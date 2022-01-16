#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

"""class of DeepWalk model."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import graphlearn as gl
import numpy as np


class DeepWalk(object):
    """
    Args:
      graph: Initialized gl.Graph object.
      node_type: User defined node type name.
      node_count: Total number of nodes.
      edge_type: User defined edge type name.
      walk_len: Random walk length.
      window_size: Window size.
      embedding_dim: Embedding dimension.
      neg_num: The number of negative samples for each node.
      temperature: Softmax temperature.
      str2hash: Set it to True if using string2hash.
      mode: train/eval/predict
    """

    def __init__(self,
                 graph,
                 node_type,
                 node_count,
                 edge_type,
                 walk_len=10,
                 window_size=5,
                 embedding_dim=64,
                 neg_num=5,
                 temperature=1.0,
                 str2hash=False,
                 mode="train"):
        self.graph = graph
        self._node_type = node_type
        self._edge_type = edge_type
        self._walk_len = walk_len
        self._window_size = window_size
        self._node_count = node_count
        self._embedding_dim = embedding_dim
        self._neg_num = neg_num
        self._temperature = temperature
        self._s2h = str2hash
        self._mode = mode

    def positive_sample(self, t):
        path = self.graph.V(self._node_type, feed=t) \
            .repeat(lambda v: v.outV(self._edge_type).sample(1).by('edge_weight'),
                    self._walk_len - 1) \
            .emit(lambda x: [x[i].ids.reshape([-1])
                             for i in range(self._walk_len)])
        src_ids, dst_ids = gl.gen_pair(path,
                                       self._window_size,
                                       self._window_size)
        return src_ids, dst_ids

    def _sample_generator(self, sample_seed):
        while True:
            try:
                batch_seed = sample_seed().next()
                if self._mode == "predict":
                    yield batch_seed.ids, batch_seed.ids.astype(np.int32)
                else:
                    src_ids, dst_ids = self.positive_sample(batch_seed.ids)
                    yield (src_ids, dst_ids), batch_seed.ids.astype(np.int32)
            except gl.errors.OutOfRangeError:
                break

    def get_output_types_shapes(self):
        if self._mode == "predict":
            output_types = (tf.int64, tf.int32)
            output_shapes = (tf.TensorShape([None]), tf.TensorShape([None]))
        else:
            output_types = ((tf.int64, tf.int64), tf.int32)
            output_shapes = ((tf.TensorShape([None]), tf.TensorShape([None])), tf.TensorShape([None]))
        return output_types, output_shapes

    def input_fn(self, data_generator):
        output_types, output_shapes = self.get_output_types_shapes()
        return tf.data.Dataset.from_generator(
            data_generator,
            output_types=output_types,
            output_shapes=output_shapes
        )

    def _encoders(self):
        src_encoder = gl.encoders.LookupEncoder(self._node_count,
                                                self._embedding_dim,
                                                init=None,
                                                str2hash=self._s2h,
                                                name='node_encoder')
        dst_encoder = gl.encoders.LookupEncoder(self._node_count,
                                                self._embedding_dim,
                                                init=tf.zeros_initializer(),
                                                str2hash=self._s2h,
                                                name='context_encoder')
        return {"src": src_encoder, "edge": None, "dst": dst_encoder}

    def model_fn(self, features, labels, mode, params, config):
        encoders = self._encoders()
        if mode == tf.estimator.ModeKeys.PREDICT:
            return tf.estimator.EstimatorSpec(
                mode=mode,
                predictions={"id": features, "predict": encoders["src"].encode(features)}
            )
        batch_size = params.get("batch_size", 64)
        src_emb = encoders["src"].encode(features[0])
        pos_dst_emb = features[1]
        loss = gl.sampled_softmax_loss(
            src_emb / self._temperature,
            pos_dst_emb,
            self._neg_num * batch_size,
            encoders["dst"].emb_table,
            encoders["dst"].bias_table,
            encoders["dst"].num,
            self._s2h
        )[0]
        if mode == tf.estimator.ModeKeys.EVAL:
            return tf.estimator.EstimatorSpec(
                mode=mode,
                loss=loss,
            )
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


def train():
    global graph
    dw = DeepWalk(
        graph=graph,
        node_type='1',
        node_count=1000,
        edge_type='1',
        walk_len=10,
        window_size=5,
        embedding_dim=64,
        neg_num=5,
        temperature=1.0,
        str2hash=False,
        mode="train"
    )
    est = tf.estimator.Estimator(
        model_fn=dw.model_fn,
        model_dir="/gpload/model_dir/dw",
        params={"batch_size": 32}
    )
    epochs = 10
    train_sample_seed = lambda: dw.graph.V("1").shuffle(traverse=True).batch(32).values()
    est.train(
        input_fn=lambda: dw.input_fn(sample_seed=train_sample_seed, epochs=epochs)
    )


def predict():
    global graph
    dw = DeepWalk(
        graph=graph,
        node_type='1',
        node_count=1000,
        edge_type='1',
        walk_len=10,
        window_size=5,
        embedding_dim=64,
        neg_num=5,
        temperature=1.0,
        str2hash=False,
        mode="predict"
    )
    est = tf.estimator.Estimator(
        model_fn=dw.model_fn,
        model_dir="/gpload/model_dir/dw"
    )
    predict_sample_seed = lambda: dw.graph.V("1").batch(32).values()
    for emb in est.predict(
        input_fn=lambda: dw.input_fn(sample_seed=predict_sample_seed, epochs=1)
    ):
        print(emb)


if __name__ == "__main__":
    from ..gnn.utils import load_graph
    graph = load_graph()
    train()
    # predict()
