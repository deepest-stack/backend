-- model info table creation

CREATE TABLE model_category(
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR(40),
    update_time timestamp DEFAULT current_timestamp
) DISTRIBUTED BY (category_id);

INSERT INTO model_category(category_id, category_name)
VALUES (100, 'keras_udm'), (101, 'linear_regression'), (102, 'logistic_regression'),
       (103, 'neural_network_classifier'), (104, 'neural_network_regressor'), (105, 'graph_neural_network'),
       (201, 'random_forest_classifier'), (202, 'random_forest_regressor'),
       (203, 'boosting_tree_classifier'), (204, 'boosting_tree_regressor'),
       (301, 'full_gcn'), (302, 'sampled_gcn'), (303, 'supervised_sage'), (304, 'unsupervised_sage'),
       (305, 'deepwalk'), (306, 'line');

CREATE TABLE model_info(
    model_id VARCHAR(40) PRIMARY KEY,
    model_category INTEGER REFERENCES model_category(category_id),
    p_model_id VARCHAR(40),
    model_arch json,
    optimizer json,
    losses json,
    metrics json,
    data_table VARCHAR,
    x_columns VARCHAR[],
    y_column VARCHAR,
    valid_data FLOAT4,
    epochs INTEGER,
    batch_size INTEGER,
    train_steps INTEGER,
    class_weight json,
    validation_steps INTEGER,
    save_path VARCHAR,
    log_table VARCHAR,
    update_time timestamp DEFAULT current_timestamp
) DISTRIBUTED BY (model_id);