#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

try:
    from debug.debug_tools import plpy
except ImportError:
    import plpy
from tensorflow.keras.callbacks import Callback
# import xgboost as xgb


class TrainLogger(Callback):

    def __init__(self, table_name):
        super(TrainLogger, self).__init__()
        self._current_epoch = 0
        self._insert_epoch_log_plan = plpy.prepare(
            "INSERT INTO %s(epoch, logs) VALUES($1, $2)" % table_name,
            ["integer", "text"]
        )
        self._insert_batch_log_plan = plpy.prepare(
            "INSERT INTO %s(epoch, batch, logs) VALUES($1, $2, $3)" % table_name,
            ["integer", "integer", "text"]
        )

    def on_batch_end(self, batch, logs=None):
        """A backwards compatibility alias for `on_train_batch_end`."""
        plpy.execute(
            self._insert_batch_log_plan,
            [self._current_epoch, batch, str(logs)]
        )

    def on_epoch_begin(self, epoch, logs=None):
        """Called at the start of an epoch.

        Subclasses should override for any actions to run. This function should only
        be called during TRAIN mode.

        Arguments:
            epoch: integer, index of epoch.
            logs: dict. Currently no data is passed to this argument for this method
              but that may change in the future.
        """
        self._current_epoch = epoch

    def on_epoch_end(self, epoch, logs=None):
        plpy.execute(
            self._insert_epoch_log_plan,
            [epoch, str(logs)]
        )


# class XGBTrainLogger(xgb.callback.TrainingCallback):
#     def __init__(self, table_name):
#         super(XGBTrainLogger, self).__init__()
#         self._plan = plpy.prepare(
#             "INSERT INTO %s(epoch, logs) VALUES($1, $2)" % table_name,
#             ["integer", "text"]
#         )
#
#     def _get_key(self, data, metric):
#         return '%s-%s' % (data, metric)
#
#     def after_iteration(self, model, epoch, evals_log):
#         plpy.execute(
#             self._plan,
#             [epoch, str(evals_log)]
#         )
#         # False to indicate training should not stop.
#         return False
