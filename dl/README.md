## Greenplum In-Database DL Tools Based On Keras




### 安装

- 安装python包

```python
bash>cd python
bash>python setup.py install  # 需要使用greenplum中所配置的python来执行安装
```


- 注册函数

```sql
bash>psql -d dev
dev=#CREATE SCHEMA dl;
dev=# \i /gpload/dl/sql/train.sql
dev=# \i /gpload/dl/sql/predict.sql
dev=# \i /gpload/dl/sql/evaluate.sql
```


### 训练内置模型

- 线性回归模型

```sql
SELECT dl_schema.train_linear_regression(
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class', -- 标签列
    16, -- epochs
    4, -- batch size
    .2 -- 验证集划分比例
    );
```

- 逻辑回归模型

```sql
SELECT dl_schema.train_logistic_regression(
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class', -- 标签列
    16, -- epochs
    4, -- batch size
    NULL, -- class weight
    .2 -- 验证集划分比例
    );
```

- 神经网络分类器

```sql
SELECT dl_schema.train_neural_network_classifier(
    ARRAY[8, 16, 8], -- 隐含层节点数
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class', -- 标签列
    16, -- epochs
    4, -- batch size
    NULL, -- class weight
    .2 -- 验证集划分比例
    );
```


- 神经网络回归器

```sql
SELECT dl_schema.train_neural_network_regressor(
    ARRAY[8, 16, 8], -- 隐含层节点数
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class', -- 标签列
    16, -- epochs
    4, -- batch size
    .2 -- 验证集划分比例
    );
```

- 随机森林分类器

```sql
SELECT dl_schema.train_random_forest_classifier(
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class', -- 标签列
    10 -- 正样本权重
    );
```

- 随机森林回归器

```sql
SELECT dl_schema.train_random_forest_regressor(
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class' -- 标签列
    );
```


- 提升树分类器

```sql
SELECT dl_schema.train_boosting_tree_classifier(
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class', -- 标签列
    32, -- epochs
    NULL -- 正样本权重
    );
```


- 提升树回归器

```sql
SELECT dl_schema.train_boosting_tree_regressor(
    'iris_data', -- 数据表
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
    'class', -- 标签列
    32 -- epochs
    );
```




### 训练自定义模型

- 通过入参定义模型

	- 定义模型的架构
	
		这种架构定义方式目前只支持`Sequential`类型的模型。通过一个二维数组来定义模型的架构，数组中的每一个元素定义了一个网络层的类型和参数，且网络层的堆叠顺序与数组中的元素保持一致。
		
		`Array[['dense', 'units=10,activation=relu'], ['dense', 'units=10,activation=relu'], ['dense', 'units=3']]`将创建一个包含3个全连接层的神经网络，其中第一个和第二个全连接层使用`relu`激活函数，第三个全连接层使用线性激活。模型架构如下。
		
		```
		Model: "sequential"
		_________________________________________________________________
		Layer (type)                 Output Shape              Param #   
		=================================================================
		dense (Dense)                (None, 10)                50        
		_________________________________________________________________
		dense_1 (Dense)              (None, 10)                110       
		_________________________________________________________________
		dense_2 (Dense)              (None, 3)                 33        
		=================================================================
		Total params: 193
		Trainable params: 193
		Non-trainable params: 0
		_________________________________________________________________
		```
		
		所支持的网络层类型参见[layer对照表](#jump1)
	
	- 定义模型的损失
	
		支持对一个模型定义多个不同的损失函数。因此，类似模型架构，通过一个二维数组来定义模型的损失，数组中的每一个元素定义了一个损失函数的类型和参数。
		
		`Array[['sparsecategoricalcrossentropy', 'from_logits=1']]`将创建一个`tf.keras.losses.SparseCategoricalCrossentropy`类型的损失函数，其`from_logits`参数为`True`（在传入布尔类型的参数时，需要通过整型数值来指定，`1`对应`True`，`0`对应`False`）。
		
		所支持的损失类型参见[loss对照表](#jump2)
		
		
	- 定义模型的优化器
	
		一个模型只能定义一个优化器，所以，通过一个一维数组来指定优化器的类型和参数。
		
		`Array['adam', 'learning_rate=0.01']`将创建一个`tf.keras.optimizers.Adam`类型的优化器，其学习率`learning_rate `为0.01。
	
		所支持的优化器类型参见[optimizer对照表](#jump3)
		
	- 定义模型的评估指标
	
		通过一个二维数据，为一个模型添加一个或者多个评估指标，数组中的每一个元素定义了一个评估指标的类型和参数。
		
		`Array[['sparsecategoricalaccuracy', '']]`将创建一个`tf.keras.metrics.SparseCategoricalAccuracy`类型的评估函数。
	
		所支持的评估指标类型参见[metric对照表](#jump4)

	- 训练
	
		指定其他必需参数后，开始训练。
		
		```sql
		dev=#SELECT dl.train(
			Array[['dense', 'units=10,activation=relu'], ['dense', 'units=10,activation=relu'], ['dense', 'units=3']],  -- 模型架构
			Array[['sparsecategoricalcrossentropy', 'from_logits=1']], -- 模型损失函数
			Array['adam', 'learning_rate=0.01'], -- 模型优化器
			Array[['sparsecategoricalaccuracy', '']], -- 模型评估指标
			'public.iris_data', -- 数据表
			Array['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
			'class', -- 标签列
			10, -- epochs
			32, -- batch size
			NULL, -- steps_per_epoch,不指定则通过batch size和数据表中的数据量来确定
			'0=1.0,1=1.0,2=1.0', -- class_weight
			NULL, -- log_table,不指定则通过生成的model_id来确定
			NULL, -- save_path,不指定则通过生成的model_id来确定
			NULL, -- valid_data,验证集比例
		)
		```
		

- 导入keras预定义的模型

	支持`Sequential `和`Functional`类型的模型，支持更加复杂的模型架构，支持对预训练后的模型进行task-specifical的训练。

	- 通过keras定义模型，将模型的架构导入数据库，模型的权重保存到文件
	
		
		```python
		>>> import tensorflow as tf
		>>> inputs = tf.keras.Input(shape=(4,))
		>>> x = tf.keras.layers.Dense(units=10, activation='relu')(inputs)
		>>> x = tf.keras.layers.Dense(units=10, activation='relu')(x)
		>>> y = tf.keras.layers.Dense(units=3, activation='relu')(x)
		>>> model = tf.keras.Model(inputs=inputs, outputs=y, name="iris_model")
		>>> model.summary()
		Model: "iris_model"
		_________________________________________________________________
		Layer (type)                 Output Shape              Param #
		=================================================================
		input_1 (InputLayer)         [(None, 4)]               0
		_________________________________________________________________
		dense_1 (Dense)              (None, 10)                50
		_________________________________________________________________
		dense_2 (Dense)              (None, 10)                110
		_________________________________________________________________
		dense_3 (Dense)              (None, 3)                 33
		=================================================================
		Total params: 193
		Trainable params: 193
		Non-trainable params: 0
		_________________________________________________________________
		```
		
		
		获取并导入模型架构
		
		```python
		>>> model.to_json()
		'{"class_name": "Model", "keras_version": "2.2.4-tf", "config": {"layers": [{"class_name": "InputLayer", "config": {"dtype": "float32", "batch_input_shape": [null, 4], "name": "input_1", "sparse": false}, "inbound_nodes": [], "name": "input_1"}, {"class_name": "Dense", "config": {"kernel_initializer": {"class_name": "GlorotUniform", "config": {"dtype": "float32", "seed": null}}, "name": "dense_1", "kernel_constraint": null, "bias_regularizer": null, "bias_constraint": null, "dtype": "float32", "activation": "relu", "trainable": true, "kernel_regularizer": null, "bias_initializer": {"class_name": "Zeros", "config": {"dtype": "float32"}}, "units": 10, "use_bias": true, "activity_regularizer": null}, "inbound_nodes": [[["input_1", 0, 0, {}]]], "name": "dense_1"}, {"class_name": "Dense", "config": {"kernel_initializer": {"class_name": "GlorotUniform", "config": {"dtype": "float32", "seed": null}}, "name": "dense_2", "kernel_constraint": null, "bias_regularizer": null, "bias_constraint": null, "dtype": "float32", "activation": "relu", "trainable": true, "kernel_regularizer": null, "bias_initializer": {"class_name": "Zeros", "config": {"dtype": "float32"}}, "units": 10, "use_bias": true, "activity_regularizer": null}, "inbound_nodes": [[["dense_1", 0, 0, {}]]], "name": "dense_2"}, {"class_name": "Dense", "config": {"kernel_initializer": {"class_name": "GlorotUniform", "config": {"dtype": "float32", "seed": null}}, "name": "dense_3", "kernel_constraint": null, "bias_regularizer": null, "bias_constraint": null, "dtype": "float32", "activation": "relu", "trainable": true, "kernel_regularizer": null, "bias_initializer": {"class_name": "Zeros", "config": {"dtype": "float32"}}, "units": 3, "use_bias": true, "activity_regularizer": null}, "inbound_nodes": [[["dense_2", 0, 0, {}]]], "name": "dense_3"}], "input_layers": [["input_1", 0, 0]], "output_layers": [["dense_3", 0, 0]], "name": "iris_model"}, "backend": "tensorflow"}'
		```
		
		```sql
		bash>psql -d dev
		dev=#INSERT INTO model_info(model_id, model_arch) VALUES('model_20201214_1', '{"class_name": "Model", "keras_version": "2.2.4-tf", "config": {"layers": [{"class_name": "InputLayer", "config": {"dtype": "float32", "batch_input_shape": [null, 4], "name": "input_1", "sparse": false}, "inbound_nodes": [], "name": "input_1"}, {"class_name": "Dense", "config": {"kernel_initializer": {"class_name": "GlorotUniform", "config": {"dtype": "float32", "seed": null}}, "name": "dense_1", "kernel_constraint": null, "bias_regularizer": null, "bias_constraint": null, "dtype": "float32", "activation": "relu", "trainable": true, "kernel_regularizer": null, "bias_initializer": {"class_name": "Zeros", "config": {"dtype": "float32"}}, "units": 10, "use_bias": true, "activity_regularizer": null}, "inbound_nodes": [[["input_1", 0, 0, {}]]], "name": "dense_1"}, {"class_name": "Dense", "config": {"kernel_initializer": {"class_name": "GlorotUniform", "config": {"dtype": "float32", "seed": null}}, "name": "dense_2", "kernel_constraint": null, "bias_regularizer": null, "bias_constraint": null, "dtype": "float32", "activation": "relu", "trainable": true, "kernel_regularizer": null, "bias_initializer": {"class_name": "Zeros", "config": {"dtype": "float32"}}, "units": 10, "use_bias": true, "activity_regularizer": null}, "inbound_nodes": [[["dense_1", 0, 0, {}]]], "name": "dense_2"}, {"class_name": "Dense", "config": {"kernel_initializer": {"class_name": "GlorotUniform", "config": {"dtype": "float32", "seed": null}}, "name": "dense_3", "kernel_constraint": null, "bias_regularizer": null, "bias_constraint": null, "dtype": "float32", "activation": "relu", "trainable": true, "kernel_regularizer": null, "bias_initializer": {"class_name": "Zeros", "config": {"dtype": "float32"}}, "units": 3, "use_bias": true, "activity_regularizer": null}, "inbound_nodes": [[["dense_2", 0, 0, {}]]], "name": "dense_3"}], "input_layers": [["input_1", 0, 0]], "output_layers": [["dense_3", 0, 0]], "name": "iris_model"}, "backend": "tensorflow"}');
		```
		
		保存模型权重，更新数据库
		
		```python
		>>> model.save_weights("/home/gpadmin/model_20201214_1")
		```
		
		```sql
		dev=#UPDATE model_info SET save_path='/home/gpadmin/model_20201214_1' WHERE model_id = 'model_20201214_1';
		```
		
		
		
	- 在数据库中加载模型，开始训练
	
		此时，不再需要传入模型架构的参数，只需要指定模型保存在数据库中的id，其他参数与通过入参定义模型的方式一致。
	
		```sql
		dev=#SELECT dl.train_from(
			'model_20201214_1', -- 模型id
			Array[['sparsecategoricalcrossentropy', 'from_logits=1']], -- 模型损失函数
			Array['adam', 'learning_rate=0.01'], -- 模型优化器
			Array[['sparsecategoricalaccuracy', '']], -- 模型评估标准
			'public.iris_data', -- 数据表
			Array['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
			'class', -- 标签列
			10, -- epochs
			32, -- batch size
			NULL, -- steps_per_epoch,不指定则通过batch size和数据表中的数据量来确定
			'0=1.0,1=1.0,2=1.0', -- class_weight
			NULL, -- log_table,不指定则通过生成的model_id来确定
			NULL, -- save_path,不指定则通过生成的model_id来确定
			NULL, -- valid_data,验证集划分比例
		)
		```


- 训练日志

	训练日志记录在`log_table`中，如果在调用`dl.train/dl.train_from`指定了`log_table`，则会以该参数作为表名，创建训练日志表。如果没有指定`log_table`，那么，可以通过`dl.train/dl.train_from`返回的`model_id`找到模型的训练日志表，表名为`${model_id}_train_log`。
	
	日志表的schema如下。
	
	```
	Unlogged table "public.model_d0a000a024206ababe118dd324d4ee6f_train_log"
	   Column    |            Type             |         Modifiers
	-------------+-----------------------------+---------------------------
	 epoch       | integer                     |
	 batch       | integer                     |
	 logs        | text                        |
	 update_time | timestamp without time zone | default clock_timestamp()
	Distributed by: (epoch)
	```
	
	`logs`字段中记录了每个`batch/epoch`时的损失和评估指标信息。
	
	```sql
	dev=# SELECT * FROM model_d0a000a024206ababe118dd324d4ee6f_train_log ORDER BY epoch,batch LIMIT 20;
	 epoch | batch |                                          logs                                           |        update_time
	-------+-------+-----------------------------------------------------------------------------------------+----------------------------
	     0 |     0 | {'sparse_categorical_accuracy': 0.3125, 'loss': 1.616716, 'batch': 0, 'size': 32}       | 2020-12-14 06:52:43.491613
	     0 |     1 | {'sparse_categorical_accuracy': 0.28125, 'loss': 1.6383431, 'batch': 1, 'size': 32}     | 2020-12-14 06:52:43.505198
	     0 |     2 | {'sparse_categorical_accuracy': 0.35416666, 'loss': 1.1127509, 'batch': 2, 'size': 32}  | 2020-12-14 06:52:43.511801
	     0 |     3 | {'sparse_categorical_accuracy': 0.4296875, 'loss': 1.0634557, 'batch': 3, 'size': 32}   | 2020-12-14 06:52:43.517686
	     0 |     4 | {'sparse_categorical_accuracy': 0.41333333, 'loss': 1.5937382, 'batch': 4, 'size': 22}  | 2020-12-14 06:52:43.523965
	     0 |       | {'sparse_categorical_accuracy': 0.41333333, 'loss': 1.3924182875951132}                 | 2020-12-14 06:52:43.528352
	     1 |     0 | {'sparse_categorical_accuracy': 0.8125, 'loss': 0.923752, 'batch': 0, 'size': 32}       | 2020-12-14 06:52:43.551481
	     1 |     1 | {'sparse_categorical_accuracy': 0.65625, 'loss': 1.150304, 'batch': 1, 'size': 32}      | 2020-12-14 06:52:43.557532
	     1 |     2 | {'sparse_categorical_accuracy': 0.6458333, 'loss': 1.0058501, 'batch': 2, 'size': 32}   | 2020-12-14 06:52:43.563323
	     1 |     3 | {'sparse_categorical_accuracy': 0.578125, 'loss': 0.9755715, 'batch': 3, 'size': 32}    | 2020-12-14 06:52:43.571368
	     1 |     4 | {'sparse_categorical_accuracy': 0.55333334, 'loss': 0.91676366, 'batch': 4, 'size': 22} | 2020-12-14 06:52:43.580312
	     1 |       | {'sparse_categorical_accuracy': 0.55333334, 'loss': 0.9996272166570027}                 | 2020-12-14 06:52:43.58473
	     2 |     0 | {'sparse_categorical_accuracy': 0.34375, 'loss': 0.93351674, 'batch': 0, 'size': 32}    | 2020-12-14 06:52:43.606228
	     2 |     1 | {'sparse_categorical_accuracy': 0.28125, 'loss': 0.99044764, 'batch': 1, 'size': 32}    | 2020-12-14 06:52:43.614861
	     2 |     2 | {'sparse_categorical_accuracy': 0.3125, 'loss': 0.90327597, 'batch': 2, 'size': 32}     | 2020-12-14 06:52:43.62288
	     2 |     3 | {'sparse_categorical_accuracy': 0.3203125, 'loss': 0.90444076, 'batch': 3, 'size': 32}  | 2020-12-14 06:52:43.630835
	     2 |     4 | {'sparse_categorical_accuracy': 0.33333334, 'loss': 0.8583016, 'batch': 4, 'size': 22}  | 2020-12-14 06:52:43.638351
	     2 |       | {'sparse_categorical_accuracy': 0.33333334, 'loss': 0.9219762015342713}                 | 2020-12-14 06:52:43.642295
	     3 |     0 | {'sparse_categorical_accuracy': 0.34375, 'loss': 0.8890148, 'batch': 0, 'size': 32}     | 2020-12-14 06:52:43.671178
	     3 |     1 | {'sparse_categorical_accuracy': 0.28125, 'loss': 0.9613821, 'batch': 1, 'size': 32}     | 2020-12-14 06:52:43.679032
	(20 rows)
	```

- 模型信息

	模型信息保存在表`model_info`中，`model_info`的schema如下。
	
	```
	                            Table "public.model_info"
	      Column      |            Type             | Collation | Nullable | Default
	------------------+-----------------------------+-----------+----------+---------
	 model_id         | character varying(40)       |           | not null |
	 model_category   | integer                     |           |          |
	 p_model_id       | character varying(40)       |           |          |
	 model_arch       | json                        |           |          |
	 optimizer        | json                        |           |          |
	 losses           | json                        |           |          |
	 metrics          | json                        |           |          |
	 data_table       | character varying           |           |          |
	 x_columns        | character varying[]         |           |          |
	 y_column         | character varying           |           |          |
	 valid_data       | real                        |           |          |
	 epochs           | integer                     |           |          |
	 batch_size       | integer                     |           |          |
	 train_steps      | integer                     |           |          |
	 class_weight     | json                        |           |          |
	 validation_steps | integer                     |           |          |
	 save_path        | character varying           |           |          |
	 log_table        | character varying           |           |          |
	 update_time      | timestamp without time zone |           |          | now()
	Indexes:
	    "model_info_pkey" PRIMARY KEY, btree (model_id)
	Foreign-key constraints:
	    "model_info_model_category_fkey" FOREIGN KEY (model_category) REFERENCES model_category(category_id)
	```
	
	model_category
	
	```sql
	dev=# SELECT * FROM model_category;
	 category_id |       category_name       |        update_time
	-------------+---------------------------+----------------------------
	         202 | random_forest_regressor   | 2021-01-11 10:11:16.617144
	         104 | neural_network_regressor  | 2021-01-11 10:11:16.617144
	         102 | logistic_regression       | 2021-01-11 10:11:16.617144
	         103 | neural_network_classifier | 2021-01-11 10:11:16.617144
	         203 | boosting_tree_classifier  | 2021-01-11 10:11:16.617144
	         101 | linear_regression         | 2021-01-11 10:11:16.617144
	         204 | boosting_tree_regressor   | 2021-01-11 10:11:16.617144
	         100 | keras_udm                 | 2021-01-11 10:11:16.617144
	         105 | graph_neural_network      | 2021-01-11 10:11:16.617144
	         201 | random_forest_classifier  | 2021-01-11 10:11:16.617144
	(10 rows)
	```
	
	可通过`dl.train/dl.train_from`返回的`model_id`查询模型的信息。



### 模型预测

支持内置模型、通过`dl.train/dl.train_from`方式训练的自定义模型或者其他方式训练的keras模型。

在指定预测数据时，需要指定数据的`id`列，用于将预测结果与对应的样本关联，在预测结果表中，`id`列以可变字符类型存储。

```sql
dev=#SELECT dl.predict(
	'model_d0a000a024206ababe118dd324d4ee6f', -- 模型id
	'iris_data', -- 预测数据表
	'id', -- 数据id列
	Array['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 数据特征列
	32, -- batch size
	'preds_d0a000a024206ababe118dd324d4ee6f', -- 结果表表名
	'class' -- 结果的类型，输出概率还是类别
)
```


```sql
dev=# SELECT iris_data.id, iris_data.class, preds_d0a000a024206ababe118dd324d4ee6f.predict FROM iris_data JOIN preds_d0a000a024206ababe118dd324d4ee6f ON ''||iris_data.id = preds_d0a000a024206ababe118dd324d4ee6f.id;
 id  | class | predict
-----+-------+---------
 142 |     1 |       1
 134 |     1 |       2
 140 |     1 |       1
  86 |     2 |       2
  68 |     2 |       2
   1 |     0 |       0
 102 |     1 |       1
 146 |     1 |       1
  11 |     0 |       0
  62 |     2 |       2
 112 |     1 |       1
 138 |     1 |       1
  77 |     2 |       2
  80 |     2 |       2
 130 |     1 |       1
 143 |     1 |       1
  15 |     0 |       0
  47 |     0 |       0
  50 |     0 |       0
  79 |     2 |       2
  46 |     0 |       0
  55 |     2 |       2
 147 |     1 |       1
  88 |     2 |       2
  91 |     2 |       2
  97 |     2 |       2
 106 |     1 |       1
  73 |     2 |       1
  85 |     2 |       2
  82 |     2 |       2
  24 |     0 |       0
  27 |     0 |       0
  89 |     2 |       2
   8 |     0 |       0
 109 |     1 |       1
 126 |     1 |       1
 139 |     1 |       1
```



### 模型评估


支持内置模型、通过`dl.train/dl.train_from`方式训练的自定义模型或者其他方式训练的keras模型。

对于使用其他方式训练的模型，需要将模型的`losses`和`optimizer`配置写入到数据库中（内置模型和使用`dl.train/dl.train_from`方式训练的模型，其`losses`和`optimizer`信息，已写入数据库，不需要手动写入），`losses`和`optimizer`配置不必与模型训练时保持一致，但需要与模型兼容。

```python
>>>json.dumps(
    {"name": model.optimizer.__class__.__name__, "config": model.optimizer.get_config()}
)
'{"config": {"beta_1": 0.9, "beta_2": 0.999, "name": "Adam", "decay": 0.0, "epsilon": 1e-07, "learning_rate": 0.01, "amsgrad": false}, "name": "Adam"}'
>>>json.dumps(
	[{"name": loss.__class__.__name__, "config": loss.get_config()} for loss in model.loss_functions]
)
'[{"config": {"reduction": "auto", "from_logits": 1, "name": null}, "name": "SparseCategoricalCrossentropy"}]'
```


```sql
dev=#UPDATE model_info SET 
	losses='[{"config": {"reduction": "auto", "from_logits": 1, "name": null}, "name": "SparseCategoricalCrossentropy"}]', 
	optimizer='{"config": {"beta_1": 0.9, "beta_2": 0.999, "name": "Adam", "decay": 0.0, "epsilon": 1e-07, "learning_rate": 0.01, "amsgrad": false}, "name": "Adam"}'
	WHERE model_id = 'model_d0a000a024206ababe118dd324d4ee6f';
```


指定评估指标`metrics`和其他参数，`metrics`的参数形式与`dl.train/dl.train_from`中一致，执行评估。

```sql
dev=#SELECT dl.evaluate(
	'model_d0a000a024206ababe118dd324d4ee6f', -- 模型id
	Array[['sparsecategoricalaccuracy', '']], -- 模型评估指标
	'iris_data', -- 评估数据集
	Array['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], -- 特征列
	'class', -- 标签列
	32, -- batch size
	NULL -- steps,不指定则通过batch size和数据表中的数据量来确定
);
 evaluate
----------
 {0.98}
(1 row)
```



### 模型部署


支持内置模型和通过`dl.train/dl.train_from`方式训练的自定义模型或者其他方式训练的keras模型。

对于使用其他方式训练的模型，需要将模型的信息（包括: `model_id`,`model_arch`,`save_path`；如果需要评估模型，还需要: `losses`,`optimizer`）写入到数据库中（内置模型和使用`dl.train/dl.train_from`方式训练的模型，其信息已写入数据库，不需要手动写入）。


### reference

组件名称不区分大小写，各组件类型的初始化参数参见keras文档。

- <span id="jump1">layer对照表</span>

|名称|类型|
|:---:|:---|
|leakyrelu|tf.keras.layers.LeakyReLU|
|prelu|tf.keras.layers.PReLU|
|elu|tf.keras.layers.ELU|
|relu|tf.keras.layers.ReLU|
|thresholdedrelu|tf.keras.layers.ThresholdedReLU|
|softmax|tf.keras.layers.Softmax|
|conv1d|tf.keras.layers.Conv1D|
|conv2d|tf.keras.layers.Conv2D|
|conv3d|tf.keras.layers.Conv3D|
|conv2dtranspose|tf.keras.layers.Conv2DTranspose|
|conv3dtranspose|tf.keras.layers.Conv3DTranspose|
|separableconv1d|tf.keras.layers.SeparableConv1D|
|separableconv2d|tf.keras.layers.SeparableConv2D|
|upsampling1d|tf.keras.layers.UpSampling1D|
|upsampling2d|tf.keras.layers.UpSampling2D|
|upsampling3d|tf.keras.layers.UpSampling3D|
|zeropadding1d|tf.keras.layers.ZeroPadding1D|
|zeropadding2d|tf.keras.layers.ZeroPadding2D|
|zeropadding3d|tf.keras.layers.ZeroPadding3D|
|cropping1d|tf.keras.layers.Cropping1D|
|cropping2d|tf.keras.layers.Cropping2D|
|cropping3d|tf.keras.layers.Cropping3D|
|masking|tf.keras.layers.Masking|
|dropout|tf.keras.layers.Dropout|
|spatialdropout1d|tf.keras.layers.SpatialDropout1D|
|spatialdropout2d|tf.keras.layers.SpatialDropout2D|
|spatialdropout3d|tf.keras.layers.SpatialDropout3D|
|activation|tf.keras.layers.Activation|
|reshape|tf.keras.layers.Reshape|
|permute|tf.keras.layers.Permute|
|flatten|tf.keras.layers.Flatten|
|repeatvector|tf.keras.layers.RepeatVector|
|lambda|tf.keras.layers.Lambda|
|dense|tf.keras.layers.Dense|
|activityregularization|tf.keras.layers.ActivityRegularization|
|additiveattention|tf.keras.layers.AdditiveAttention|
|attention|tf.keras.layers.Attention|
|embedding|tf.keras.layers.Embedding|
|locallyconnected1d|tf.keras.layers.LocallyConnected1D|
|locallyconnected2d|tf.keras.layers.LocallyConnected2D|
|add|tf.keras.layers.Add|
|subtract|tf.keras.layers.Subtract|
|multiply|tf.keras.layers.Multiply|
|average|tf.keras.layers.Average|
|maximum|tf.keras.layers.Maximum|
|minimum|tf.keras.layers.Minimum|
|concatenate|tf.keras.layers.Concatenate|
|dot|tf.keras.layers.Dot|
|alphadropout|tf.keras.layers.AlphaDropout|
|gaussiannoise|tf.keras.layers.GaussianNoise|
|gaussiandropout|tf.keras.layers.GaussianDropout|
|layernormalization|tf.keras.layers.LayerNormalization|
|batchnormalization|tf.keras.layers.BatchNormalization|
|maxpooling1d|tf.keras.layers.MaxPooling1D|
|maxpooling2d|tf.keras.layers.MaxPooling2D|
|maxpooling3d|tf.keras.layers.MaxPooling3D|
|averagepooling1d|tf.keras.layers.AveragePooling1D|
|averagepooling2d|tf.keras.layers.AveragePooling2D|
|averagepooling3d|tf.keras.layers.AveragePooling3D|
|globalaveragepooling1d|tf.keras.layers.GlobalAveragePooling1D|
|globalaveragepooling2d|tf.keras.layers.GlobalAveragePooling2D|
|globalaveragepooling3d|tf.keras.layers.GlobalAveragePooling3D|
|globalmaxpooling1d|tf.keras.layers.GlobalMaxPooling1D|
|globalmaxpooling2d|tf.keras.layers.GlobalMaxPooling2D|
|globalmaxpooling3d|tf.keras.layers.GlobalMaxPooling3D|
|rnn|tf.keras.layers.RNN|
|abstractrnncell|tf.keras.layers.AbstractRNNCell|
|stackedrnncells|tf.keras.layers.StackedRNNCells|
|simplernncell|tf.keras.layers.SimpleRNNCell|
|simplernn|tf.keras.layers.SimpleRNN|
|gru|tf.keras.layers.GRU|
|grucell|tf.keras.layers.GRUCell|
|lstm|tf.keras.layers.LSTM|
|lstmcell|tf.keras.layers.LSTMCell|
|convlstm2d|tf.keras.layers.ConvLSTM2D|
|cudnnlstm|tf.keras.layers.CuDNNLSTM|
|cudnngru|tf.keras.layers.CuDNNGRU|
|wrapper|tf.keras.layers.Wrapper|
|bidirectional|tf.keras.layers.Bidirectional|
|timedistributed|tf.keras.layers.TimeDistributed|



- <span id="jump2">loss对照表</span>

|名称|类型|
|:---:|:---|
|binarycrossentropy|tf.keras.losses.BinaryCrossentropy|
|categoricalcrossentropy|tf.keras.losses.CategoricalCrossentropy|
|categoricalhinge|tf.keras.losses.CategoricalHinge|
|cosinesimilarity|tf.keras.losses.CosineSimilarity|
|hinge|tf.keras.losses.Hinge|
|huber|tf.keras.losses.Huber|
|kldivergence|tf.keras.losses.KLDivergence|
|logcosh|tf.keras.losses.LogCosh|
|loss|tf.keras.losses.Loss|
|meanabsoluteerror|tf.keras.losses.MeanAbsoluteError|
|meanabsolutepercentageerror|tf.keras.losses.MeanAbsolutePercentageError|
|meansquarederror|tf.keras.losses.MeanSquaredError|
|meansquaredlogarithmicerror|tf.keras.losses.MeanSquaredLogarithmicError|
|poisson|tf.keras.losses.Poisson|
|sparsecategoricalcrossentropy|tf.keras.losses.SparseCategoricalCrossentropy|
|squaredhinge|tf.keras.losses.SquaredHinge|


- <span id="jump3">optimizer对照表</span>

|名称|类型|
|:---:|:---|
|sgd|tf.keras.optimizers.SGD|
|rmsprop|tf.keras.optimizers.RMSprop|
|adagrad|tf.keras.optimizers.Adagrad|
|adadelta|tf.keras.optimizers.Adadelta|
|adam|tf.keras.optimizers.Adam|
|adamax|tf.keras.optimizers.Adamax|
|nadam|tf.keras.optimizers.Nadam|
|ftrl|tf.keras.optimizers.Ftrl|


- <span id="jump4">metric对照表</span>

|名称|类型|
|:---:|:---|
|auc|tf.keras.metrics.AUC|
|accuracy|tf.keras.metrics.Accuracy|
|binaryaccuracy|tf.keras.metrics.BinaryAccuracy|
|binarycrossentropy|tf.keras.metrics.BinaryCrossentropy|
|categoricalaccuracy|tf.keras.metrics.CategoricalAccuracy|
|categoricalcrossentropy|tf.keras.metrics.CategoricalCrossentropy|
|categoricalhinge|tf.keras.metrics.CategoricalHinge|
|cosinesimilarity|tf.keras.metrics.CosineSimilarity|
|falsenegatives|tf.keras.metrics.FalseNegatives|
|falsepositives|tf.keras.metrics.FalsePositives|
|hinge|tf.keras.metrics.Hinge|
|kldivergence|tf.keras.metrics.KLDivergence|
|logcosherror|tf.keras.metrics.LogCoshError|
|mean|tf.keras.metrics.Mean|
|meanabsoluteerror|tf.keras.metrics.MeanAbsoluteError|
|meanabsolutepercentageerror|tf.keras.metrics.MeanAbsolutePercentageError|
|meaniou|tf.keras.metrics.MeanIoU|
|meanrelativeerror|tf.keras.metrics.MeanRelativeError|
|meansquarederror|tf.keras.metrics.MeanSquaredError|
|meansquaredlogarithmicerror|tf.keras.metrics.MeanSquaredLogarithmicError|
|meantensor|tf.keras.metrics.MeanTensor|
|metric|tf.keras.metrics.Metric|
|poisson|tf.keras.metrics.Poisson|
|precision|tf.keras.metrics.Precision|
|recall|tf.keras.metrics.Recall|
|rootmeansquarederror|tf.keras.metrics.RootMeanSquaredError|
|sensitivityatspecificity|tf.keras.metrics.SensitivityAtSpecificity|
|sparsecategoricalaccuracy|tf.keras.metrics.SparseCategoricalAccuracy|
|sparsecategoricalcrossentropy|tf.keras.metrics.SparseCategoricalCrossentropy|
|sparsetopkcategoricalaccuracy|tf.keras.metrics.SparseTopKCategoricalAccuracy|
|specificityatsensitivity|tf.keras.metrics.SpecificityAtSensitivity|
|squaredhinge|tf.keras.metrics.SquaredHinge|
|sum|tf.keras.metrics.Sum|
|topkcategoricalaccuracy|tf.keras.metrics.TopKCategoricalAccuracy|
|truenegatives|tf.keras.metrics.TrueNegatives|
|truepositives|tf.keras.metrics.TruePositives|
