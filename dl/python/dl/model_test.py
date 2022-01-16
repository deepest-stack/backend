#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8


from . import *


def run_train():
    return train(
        layers_def=None,
        losses=None,
        optimizer=None,
        metrics=None,
        train_table="iris_data",
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
        epochs=None,
        batch_size=16,
        steps_per_epoch=None,
        class_weight=None,
        log_table=None,
        save_path=None,
        valid_data=.3
    )


def run_train_from():
    return train_from(
        model_id="model_200011ca24209deabe115734a88db0af",
        losses=None,
        optimizer=None,
        metrics=None,
        train_table="iris_data",
        x_columns=None,
        y_column=None,
        epochs=None,
        batch_size=4,
        steps_per_epoch=None,
        class_weight=None,
        log_table=None,
        save_path=None,
        valid_data=None
    )


def run_predict():
    return predict(
        model_id="model_200011ca2420568bbe114f34c3abe44a",
        data_table="iris_data",
        x_columns=None,
        id_column="id",
        batch_size=1024,
        result_table="t1",
        result_type="class"
    )


def run_evaluate():
    return evaluate(
        model_id="model_200011ca2420d3f8be116734ed5ddd8f",
        data_table="iris_data",
        x_columns=None,
        y_column="class",
        batch_size=1024,
        steps=None,
        metrics=None
    )


def run_train_linear_regression():
    return train_linear_regression(
        train_table='iris_data',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
        epochs=16,
        batch_size=4,
        valid_data=.2
    )


def run_train_logistic_regression():
    return train_logistic_regression(
        train_table='t3',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
        epochs=16,
        batch_size=4,
        class_weight=None,
        valid_data=.2
    )


def run_train_neural_network_classifier():
    return train_neural_network_classifier(
        hidden_units=[8, 16, 8],
        train_table='public.iris_data',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
        epochs=16,
        batch_size=4,
        class_weight=None,
        valid_data=None
    )


def run_train_neural_network_regressor():
    return train_neural_network_regressor(
        hidden_units=[8, 16, 8],
        train_table='public.iris_data',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
        epochs=16,
        batch_size=4,
        valid_data=None
    )


def run_train_random_forest_regressor():
    return train_random_forest_regressor(
        train_table='public.iris_data',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
    )


def run_train_random_forest_classifier():
    return train_random_forest_classifier(
        train_table='public.iris_data',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class"
    )


def run_train_boosting_tree_regressor():
    return train_boosting_tree_regressor(
        train_table='public.iris_data',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
        epochs=32
    )


def run_train_boosting_tree_classifier():
    return train_boosting_tree_classifier(
        train_table='public.iris_data',
        x_columns=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        y_column="class",
        epochs=32
    )


def run_keras_predict():
    return predict(
        model_id="model_200011ca24206caabe111f44ed84084e",
        data_table="iris_data",
        x_columns=None,
        id_column="id",
        batch_size=1024,
        result_table="t1",
        result_type="class"
    )


def run_keras_evaluate():
    return evaluate(
        model_id="model_200011ca24206caabe111f44ed84084e",
        data_table="iris_data",
        x_columns=None,
        y_column="class",
        batch_size=1024,
        steps=None,
        metrics=None
    )


def run_xgboost_predict():
    return predict(
        model_id="model_200011ca24207ac8be113f44062324d8",
        data_table="iris_data",
        x_columns=None,
        id_column="id",
        batch_size=1024,
        result_table=None,
        result_type=None
    )


def run_xgboost_evaluate():
    return evaluate(
        model_id="model_200011ca24207ac8be113f44062324d8",
        data_table="iris_data",
        x_columns=None,
        y_column=None,
        batch_size=None,
        steps=None,
        metrics=None
    )


if __name__ == "__main__":
    # print(run_train_from())
    # run_predict()
    # print(run_train())
    # print(run_evaluate())
    # print(run_train_linear_regression())
    # print(run_train_logistic_regression())
    # print(run_train_neural_network_classifier())
    # print(run_train_neural_network_regressor())
    # print(run_train_random_forest_regressor())
    # print(run_train_random_forest_classifier())
    # print(run_train_boosting_tree_regressor())
    # print(run_train_boosting_tree_classifier())
    # print(run_keras_predict())
    # print(run_keras_evaluate())
    print(run_xgboost_predict())
    print(run_xgboost_evaluate())

