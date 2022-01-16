#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import graphlearn as gl
import tensorflow as tf
import numpy as np


class LINE(object):
    """
    Args:
      graph: Initialized gl.Graph object.
      node_type: User defined node type name.
      node_count: Total number of nodes.
      edge_type: User defined edge type name.
      embedding_dim: Embedding dimension.
      neg_num: The number of negative samples for each node.
      proximity: Set to 'first_order' or 'second_order'.
      temperature: Softmax temperature.
      str2hash: Set it to True if using string2hash.
      mode: train/eval/predict
    """

    def __init__(self,
                 graph,
                 node_type,
                 node_count,
                 edge_type,
                 embedding_dim=64,
                 neg_num=5,
                 proximity='first_order',
                 temperature=1.0,
                 str2hash=False,
                 mode="train"):
        self.graph = graph
        self._node_type = node_type
        self._edge_type = edge_type
        self._node_count = node_count
        self._embedding_dim = embedding_dim
        self._neg_num = neg_num
        self._proximity = proximity
        self._temperature = temperature
        self._s2h = str2hash
        self._mode = mode

    def positive_sample(self, t):
        return self.graph.V(self._node_type, feed=t) \
            .outV(self._edge_type).sample(1).by("random")\
            .outV(self._edge_type).sample(1).by("random")\
            .emit(lambda x: x[-1].ids.reshape((-1,)))

    def negative_sample(self, t):
        return self.graph.V(self._node_type, feed=t) \
            .alias('vSrc').outNeg(self._edge_type) \
            .sample(self._neg_num).by("in_degree").alias('vNeg') \
            .emit(lambda x: x['vNeg'].ids)

    def _encoders(self):
        src_encoder = gl.encoders.LookupEncoder(self._node_count,
                                                self._embedding_dim,
                                                init=None,
                                                str2hash=self._s2h,
                                                name='first_encoder')
        if self._proximity == 'first_order':
            dst_encoder = src_encoder
        elif self._proximity == 'second_order':
            dst_encoder = gl.encoders.LookupEncoder(self._node_count,
                                                    self._embedding_dim,
                                                    init=tf.zeros_initializer(),
                                                    str2hash=self._s2h,
                                                    name='second_encoder')
        else:
            raise Exception("no encoder implemented!")

        return {"src": src_encoder, "edge": None, "dst": dst_encoder}

    def _first_order_loss(self, src_emb, pos_dst_emb, neg_dst_emb):
        return gl.kl_loss(src_emb, pos_dst_emb, neg_dst_emb)

    def _second_order_loss(self, src_emb, pos_dst_emb, dst_encoder, batch_size=64):
        return gl.sampled_softmax_loss(
            src_emb / self._temperature,
            pos_dst_emb,
            self._neg_num * batch_size,
            dst_encoder.emb_table,
            dst_encoder.bias_table,
            dst_encoder.num,
            self._s2h
        )

    def _sample_generator(self, sample_seed):
        while True:
            try:
                batch_seed = sample_seed().next()
                if self._mode == "predict":
                    yield batch_seed.ids, batch_seed.ids.astype(np.int32)
                else:
                    if self._proximity == "second_order":
                        pos_dst_ids = self.positive_sample(batch_seed.ids)
                    else:
                        pos_dst_ids = batch_seed.ids
                    neg_dst_ids = self.negative_sample(batch_seed.ids)
                    yield (batch_seed.ids, pos_dst_ids, neg_dst_ids), batch_seed.ids.astype(np.int32)
            except gl.errors.OutOfRangeError:
                break

    def get_output_types_shapes(self):
        if self._mode == "predict":
            output_types = (tf.int64, tf.int32)
            output_shapes = (tf.TensorShape([None]), tf.TensorShape([None]))
        else:
            output_types = ((tf.int64, tf.int64, tf.int64), tf.int32)
            output_shapes = ((tf.TensorShape([None]), tf.TensorShape([None]), tf.TensorShape([None, self._neg_num])),
                             tf.TensorShape([None]))
        return output_types, output_shapes

    def input_fn(self, data_generator):
        output_types, output_shapes = self.get_output_types_shapes()
        return tf.data.Dataset.from_generator(
            data_generator,
            output_types=output_types,
            output_shapes=output_shapes
        )

    def model_fn(self, features, labels, mode, params, config):
        encoders = self._encoders()
        if mode == tf.estimator.ModeKeys.PREDICT:
            return tf.estimator.EstimatorSpec(
                mode=mode,
                predictions={"id": features, "predict": encoders["src"].encode(features)}
            )
        src_emb = encoders["src"].encode(features[0])
        loss = self._first_order_loss(
            src_emb,
            encoders["dst"].encode(features[0]),
            encoders["dst"].encode(features[2])
        )[0]
        if self._proximity == "second_order":
            batch_size = params.get("batch_size", 64)
            loss = loss + self._second_order_loss(
                src_emb,
                features[1],
                encoders["dst"],
                batch_size
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
    line = LINE(
        graph=graph,
        node_type='1',
        node_count=1000,
        edge_type='1',
        proximity='second_order',
        embedding_dim=64,
        neg_num=5,
        temperature=1.0,
        str2hash=False,
        mode="train"
    )
    est = tf.estimator.Estimator(
        model_fn=line.model_fn,
        # model_dir="/gpload/model_dir/line1st",
        model_dir="/gpload/model_dir/line2nd",
        params={"batch_size": 32}
    )
    epochs = 1
    train_sample_seed = lambda: line.graph.V("1").shuffle(traverse=True).batch(32).values()
    est.train(
        input_fn=lambda: line.input_fn(sample_seed=train_sample_seed, epochs=epochs)
    )


def predict():
    global graph
    line = LINE(
        graph=graph,
        node_type='1',
        node_count=1000,
        edge_type='1',
        proximity='second_order',
        embedding_dim=64,
        neg_num=5,
        temperature=1.0,
        str2hash=False,
        mode="predict"
    )
    est = tf.estimator.Estimator(
        model_fn=line.model_fn,
        model_dir="/gpload/model_dir/line2nd"
    )
    predict_sample_seed = lambda: line.graph.V("1").batch(32).values()
    for emb in est.predict(
        input_fn=lambda: line.input_fn(sample_seed=predict_sample_seed, epochs=1)
    ):
        print(emb)


if __name__ == "__main__":
    from ..gnn.utils import load_graph
    graph = load_graph()
    train()
    # predict()