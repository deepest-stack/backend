#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

try:
    from debug.debug_tools import plpy
except ImportError:
    import plpy
import os
import uuid
import json
from .conf import MODEL_SAVE_PATH
import re
import graphlearn as gl
import pandas as pd
int_pattern = re.compile("^-?[1-9]\d*$")
float_pattern = re.compile("^-?([1-9]\d*\.\d*|0\.\d*[1-9]\d*|0?\.0+|0)$")


def str2dict(str_, delimiter=','):
    str_split = str_.split(delimiter)
    rtn = dict()
    for kv in str_split:
        if not kv.strip(): continue
        k, v = kv.split('=')
        if int_pattern.search(v.strip()):
            v = int(v.strip())
        elif float_pattern.search(v.strip()):
            v = float(v.strip())
        else:
            v = v.strip()
        rtn[k.strip()] = v
    return rtn


def write_model_info(model_id, model_category, p_model_id, arch, optimizer, losses, metrics,
                      train_table, x_columns, y_column, valid_data, epochs, batch_size, steps_per_epoch,
                      class_weight, validation_steps, save_path, log_table):
    plan_ = plpy.prepare(
        """
        INSERT INTO model_info(
            model_id,
            model_category,
            p_model_id, 
            model_arch, 
            optimizer, 
            losses,
            metrics,
            data_table,
            x_columns,
            y_column,
            valid_data,
            epochs,
            batch_size,
            train_steps,
            class_weight,
            validation_steps,
            save_path,
            log_table)
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        """,
        ["VARCHAR(40)", "INTEGER", "VARCHAR(40)", "json", "json", "json", "json",
         "VARCHAR", "VARCHAR[]", "VARCHAR", "REAL", "INTEGER", "INTEGER", "INTEGER",
         "json", "INTEGER", "VARCHAR", "VARCHAR"]
    )
    plpy.execute(
        plan_,
        [model_id, model_category, p_model_id, arch, optimizer, losses, metrics,
         train_table, x_columns, y_column, valid_data, epochs, batch_size, steps_per_epoch,
         class_weight, validation_steps, save_path, log_table]
    )


def query_model_info(model_id, *columns):
    json_fields = {"model_arch", "optimizer", "losses", "metrics", "class_weight"}
    sql = "SELECT %s FROM model_info WHERE model_id = '%s' LIMIT 1" % (", ".join(columns), model_id)
    result = plpy.execute(sql)
    model_info = dict()
    if len(result) >= 1:
        for k, v in result[0].items():
            if plpy.__class__.__name__ == 'PlPython':
                model_info[k] = v
            else:
                model_info[k] = json.loads(v) if k in json_fields and v else v
    else:
        raise RuntimeError("model: %s not exists" % model_id)
    return [model_info, ]


def create_log_table(table_name):
    # clock_timestamp rather than current_timestamp which will
    # always be begin time of transaction
    plpy.execute("""CREATE UNLOGGED TABLE 
                    %s(epoch INT, batch INT, logs TEXT, update_time timestamp DEFAULT clock_timestamp())
                    DISTRIBUTED BY (epoch)""" % table_name)


def gen_model_id():
    return "model_" + uuid.uuid1().hex[::-1]


def get_save_path(save_path, model_id):
    if save_path is None:
        save_path = MODEL_SAVE_PATH + "/%s" % model_id
    # remove double slash in path since it will cause error when load model
    return os.path.normpath(save_path)


def get_log_table(log_table, model_id):
    if log_table is None:
        log_table = model_id + "_train_log"
    return log_table


def data_generator(table_name, x_columns, y_column, num_rows, order_by=None):
    columns = x_columns + [y_column, ]
    sql = "SELECT %s FROM %s" % (",  ".join(columns), table_name)
    if order_by: sql = sql + " ORDER BY %s" % order_by
    cursor = plpy.cursor(sql)
    rows = cursor.fetch(num_rows)
    while rows:
        # x, y = [], []
        # for row in rows:
        #     x.append([row[c] for c in x_columns])
        #     y.append(row[y_column])
        # yield (np.array(x), np.array(y))
        df = pd.DataFrame(rows[:])
        yield (df[x_columns].to_numpy(), df[y_column].to_numpy())
        rows = cursor.fetch(num_rows)
    cursor.close()


def get_db_connect_arg():
    connect_arg = dict()
    # connect_arg["gp_user"] = plpy.execute("SELECT current_user")[0]["current_user"]
    # connect_arg["gp_host"] = plpy.execute("SELECT inet_server_addr()")[0]["inet_server_addr"]
    # connect_arg["gp_port"] = int(plpy.execute("SELECT inet_server_port()")[0]["inet_server_port"])
    # connect_arg["gp_dbname"] = plpy.execute("SELECT current_database()")[0]["current_database"]
    connect_arg["gp_host"] = "192.168.8.138"
    connect_arg["gp_port"] = 35432
    connect_arg["gp_user"] = "gpadmin"
    connect_arg["gp_dbname"] = "dev"

    return connect_arg


def build_graph(nodes_def, edges_def, redis_host, redis_port, redis_passwd, init_redis=False, **redis_kwargs):
    kwargs = get_db_connect_arg()
    kwargs["redis_host"] = redis_host
    kwargs["redis_port"] = redis_port
    kwargs["redis_passwd"] = redis_passwd
    kwargs["init_redis"] = init_redis
    kwargs.update(redis_kwargs)
    g = gl.Graph(**kwargs)
    for node_def in json.loads(nodes_def):
        g = g.node(
            "",
            node_type=node_def["node_type"],
            decoder=gl.Decoder(
                labeled=node_def.get("labeled", False),
                label_column=node_def.get("label_column", "label"),
                attr_types=node_def.get("attr_types"),
                attr_columns=node_def.get("attr_columns"),
                weighted=node_def.get("weighted", False),
                weight_column=node_def.get("weight_column", "weight")
            )
        )
    for edge_def in json.loads(edges_def):
        g = g.edge(
            "",
            edge_type=tuple(edge_def["edge_type"]),
            decoder=gl.Decoder(
                weighted=edge_def.get("weighted", False),
                weight_column=edge_def.get("weight_column", "weight")
            ),
            directed=edge_def.get("directed", False)
        )
    return g.init()


if __name__ == "__main__":
    print(get_db_connect_arg())


