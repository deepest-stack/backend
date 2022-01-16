#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8


from .xgboost_models import predict as xgboost_predict
from .xgboost_models import evaluate as xgboost_evaluate
from .keras_models import predict as keras_predict
from .keras_models import evaluate as keras_evaluate
from .graph_models import predict as graph_predict
from .graph_models import evaluate as graph_evaluate
from .utils import query_model_info


def predict(model_id, data_table, id_column, x_columns, batch_size, result_table, result_type, **kwargs):
    model_category = query_model_info(model_id, "model_category")[0]["model_category"]
    # keras model
    if 100 <= model_category < 200:
        return keras_predict(model_id, data_table, id_column, x_columns, batch_size, result_table, result_type)
    # xgboost model
    elif 200 <= model_category < 300:
        return xgboost_predict(model_id, data_table, id_column, x_columns, batch_size, result_table)
    # graph model
    elif 300 <= model_category < 400:
        node_type = kwargs.pop("node_type")
        redis_host = kwargs.pop("redis_host")
        redis_port = kwargs.pop("redis_port")
        result_type = kwargs.pop("result_type")
        return graph_predict(model_id, node_type, redis_host, redis_port, result_type, **kwargs)
    else:
        pass


def evaluate(model_id, metrics, data_table, x_columns, y_column, batch_size, steps, **kwargs):
    model_category = query_model_info(model_id, "model_category")[0]["model_category"]
    # keras model
    if 100 <= model_category < 200:
        return keras_evaluate(model_id, metrics, data_table, x_columns, y_column, batch_size, steps)
    # xgboost model
    elif 200 <= model_category < 300:
        return xgboost_evaluate(model_id, data_table, x_columns, y_column, batch_size)
    # graph model
    elif 300 <= model_category < 400:
        node_type = kwargs.pop("node_type")
        redis_host = kwargs.pop("redis_host")
        redis_port = kwargs.pop("redis_port")
        return graph_evaluate(model_id, node_type, redis_host, redis_port, **kwargs)
    else:
        pass







