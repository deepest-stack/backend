CREATE OR REPLACE FUNCTION DL_SCHEMA.train(
    layers_def      VARCHAR[][2],
    losses          VARCHAR[][2],
    optimizer       VARCHAR[2],
    metrics         VARCHAR[][2],
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER,
    batch_size      INTEGER,
    steps_per_epoch INTEGER,
    class_weight    VARCHAR,
    log_table       VARCHAR,
    save_path       VARCHAR,
    valid_data      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train
    return train(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_from(
    model_id        VARCHAR,
    losses          VARCHAR[][2],
    optimizer       VARCHAR[2],
    metrics         VARCHAR[][2],
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER,
    batch_size      INTEGER,
    steps_per_epoch INTEGER,
    class_weight    VARCHAR,
    log_table       VARCHAR,
    save_path       VARCHAR,
    valid_data      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train_from
    return train_from(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_linear_regression(
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER,
    batch_size      INTEGER,
    valid_data      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train_linear_regression
    return train_linear_regression(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_logistic_regression(
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER,
    batch_size      INTEGER,
    class_weight    VARCHAR,
    valid_data      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train_logistic_regression
    return train_logistic_regression(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_neural_network_classifier(
    hidden_units    INTEGER[],
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER,
    batch_size      INTEGER,
    class_weight    VARCHAR,
    valid_data      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train_neural_network_classifier
    return train_neural_network_classifier(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_neural_network_regressor(
    hidden_units    INTEGER[],
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER,
    batch_size      INTEGER,
    valid_data      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train_neural_network_regressor
    return train_neural_network_regressor(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_random_forest_classifier(
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    positive_class_weight      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train_random_forest_classifier
    return train_random_forest_classifier(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_random_forest_regressor(
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR
) RETURNS VARCHAR AS $$
    from dl import train_random_forest_regressor
    return train_random_forest_regressor(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_boosting_tree_classifier(
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER,
    positive_class_weight      FLOAT4
) RETURNS VARCHAR AS $$
    from dl import train_boosting_tree_classifier
    return train_boosting_tree_classifier(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION DL_SCHEMA.train_boosting_tree_regressor(
    train_table     VARCHAR,
    x_columns       VARCHAR[],
    y_column        VARCHAR,
    epochs          INTEGER
) RETURNS VARCHAR AS $$
    from dl import train_boosting_tree_regressor
    return train_boosting_tree_regressor(**globals())
$$ LANGUAGE plpythonu VOLATILE EXECUTE ON MASTER;
-------------------------------------------------------------------------