CREATE OR REPLACE FUNCTION DL_SCHEMA.predict(
    model_id        VARCHAR,
    data_table      VARCHAR,
    id_column       VARCHAR,
    x_columns       VARCHAR[],
    batch_size      INTEGER,
    result_table    VARCHAR,
    result_type     VARCHAR
) RETURNS VARCHAR AS $$
    from dl import predict
    return predict(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
