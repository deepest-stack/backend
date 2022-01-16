CREATE OR REPLACE FUNCTION DL_SCHEMA.evaluate(
    model_id        VARCHAR,
    metrics         VARCHAR[][2],
    data_table      VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    batch_size      INTEGER,
    steps           INTEGER
) RETURNS REAL[] AS $$
    from dl import evaluate
    return evaluate(**globals())
$$ LANGUAGE plpythonu STABLE EXECUTE ON MASTER;
