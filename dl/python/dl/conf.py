#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

import tensorflow as tf
import os


DEFAULT_BATCH_SIZE = 1024
DEFAULT_EPOCHS = 10
DEFAULT_LAYER_DEF = [
    ['dense', 'units=32, activation=tanh'],
    ['dense', 'units=64, activation=tanh'],
    ['dense', 'units=64, activation=tanh'],
    ['dense', 'units=1'],
]
DEFAULT_LOSS = [['meansquarederror', ''], ]
DEFAULT_OPTIMIZER = ['adam', '']
DEFAULT_METRICS = [['meansquarederror', ''], ]
DEFAULT_VALIDATION = .25


MODEL_CATEGORY = {
    "keras_udm": 100,
    "linear_regression": 101,
    "logistic_regression": 102,
    "neural_network_classifier": 103,
    "neural_network_regressor": 104,
    "graph_neural_network": 105,
    "random_forest_classifier": 201,
    "random_forest_regressor": 202,
    "boosting_tree_classifier": 203,
    "boosting_tree_regressor": 204,
    "full_gcn": 301,
    "sampled_gcn": 302,
    "supervised_sage": 303,
    "unsupervised_sage": 304,
    "deepwalk": 305,
    "line": 306
}


GPU_AVAILABLE = tf.test.is_gpu_available()

MODEL_SAVE_PATH = os.environ.get("MODEL_SAVE_PATH", "/tmp")
