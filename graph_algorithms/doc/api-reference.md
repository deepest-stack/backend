

- `lookup_vertices(
    IN vids INTEGER[], 
    IN vtype VARCHAR,
    IN attr_columns VARCHAR[],
    IN result_table_name VARCHAR
)`
	 
	查找顶点的属性。
	
	- Parameters
	
		vids，整型数组，指定顶点的id
		
		vtype，可变长度字符，指定顶点的类型
		
		attr_columns，字符数组，指定需要查找的字段
		
		result_table_name，可变长度字符，指定结果表的表名，结果表包含两个字段：`vid`，整型数值，顶点的id；`attrs`，JSON格式的顶点属性
	
	- Returns
	
		查找到的顶点的数量
	
	- Examples
	
	```
	gnn_dev=# SELECT ldbc.lookup_vertices(array[945, 236], 'person', array['cate2', 'cate7'], 't1');
	 lookup_vertices
	-----------------
	               2
	(1 row)
	gnn_dev=# SELECT * FROM t1;
	 vid |             attrs
	-----+-------------------------------
	 945 | {"cate2" : 4, "cate7" : "14"}
	 236 | {"cate2" : 0, "cate7" : "10"}
	(2 rows)
	```




- `lookup_edges(
    IN src_ids INTEGER[], 
    IN dst_ids INTEGER[],
    IN etype VARCHAR,
    IN attr_columns VARCHAR[],
    IN result_table_name VARCHAR
)`

	查找边的属性。

	- Parameters
	
		src_ids，整型数组，指定边的起始顶点id
		
		dst_ids，整型数组，指定边的目的顶点id
		
		etype，可变长度字符，指定边的类型
		
		attr_columns，字符数组，指定需要查找的字段
		
		result_table_name，可变长度字符，指定结果表的表名，结果表包含三个字段：`src_id`，整型数值，边的起始顶点id；`dst_id`，整型数值，边的目的顶点id；`attrs`，JSON格式的边属性
	
	- Returns
	
		查找到的边的数量
	
	- Examples
	
	```
	gnn_dev=# SELECT ldbc.lookup_edges(array[9880, 926, 4708], array[2929974, 2543638, 1560677], 'likes', array['weight'], 't1');
	 lookup_edges
	--------------
	            3
	(1 row)
	gnn_dev=# SELECT * FROM t1;
	 src_id | dst_id  |             attrs
	--------+---------+--------------------------------
	   9880 | 2929974 | {"weight" : 0.414501078892499}
	    926 | 2543638 | {"weight" : 0.12310382258147}
	   4708 | 1560677 | {"weight" : 0.832381797488779}
	(3 rows)
	```


![graph_demo.png](../resources/graph_demo.png)

- `paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN result_table_name VARCHAR
)`
	 
	从指定节点开始直到叶子节点的*所有路径*。
	
	在上图中，从A点开始的所有路径包括：  
	*A-->B*  
	*A-->E-->G*  
	*A-->C-->E-->G*  
	*A-->C-->F*  
	*A-->D-->F*  
	
	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT paths_from(ARRAY[35, 20, 109], 16, 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		运行时间依赖于路径的长度，与传入参数中指定的起始节点的数量无关，因此，如果需要搜索多个节点的路径，建议通过数组的方式一次传入多个节点id，而不是每次传入一个节点id、多次调用函数。
		
		如果两个节点之间存在重复的边，则该条重复的边以及路径上后续所有的边，都会重复出现在结果中。如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。
		
		对于只考虑特定类型的边的情形，参见[指定边的类型](#jump1)。


- `paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
)`

	从指定节点开始直到叶子节点的*所有路径*以及路径上边的属性。

	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		attr_column，可变长度字符，指定边表中定义边的属性的字段名称
		
		attr_type，可变长度字符，指定边表中边的属性的类型
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT paths_from(ARRAY[35, 20, 109], 16, 'edge_weight', 'FLOAT', 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		运行时间依赖于路径的长度，与传入参数中指定的起始节点的数量无关，因此，如果需要搜索多个节点的路径，建议通过数组的方式一次传入多个节点id，而不是每次传入一个节点id、多次调用函数。
		
		如果两个节点之间存在重复的边，则该条重复的边以及路径上后续所有的边，都会重复出现在结果中。如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。
		
		目前只支持数值类型的属性，对于长度小于最大长度的路径，返回的属性数组中的最后一个数值无意义（值为NULL），如果长度等于最大路径长度，属性数组中的最后一个数值为路径上最后一条边的属性。
		
		对于只考虑特定类型的边的情形，参见[指定边的类型](#jump3)。
	
- <span id="jump1">`paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
)`</span>

	从指定节点开始，沿着指定类型的边，直到叶子节点的*所有路径*。
	
	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称
		
		edge_type_value，整型数值，指定边的类型
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT paths_from(ARRAY[35, 20, 109], 16, 'edge_type', 1, 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。
		
		如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。


- <span id="jump3">`paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
)`</span>

	从指定节点开始，沿着指定类型的边，直到叶子节点的*所有路径*以及路径上边的属性。
	
	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称
		
		edge_type_value，整型数值，指定边的类型
		
		attr_column，可变长度字符，指定边表中定义边的属性的字段名称
		
		attr_type，可变长度字符，指定边表中边的属性的类型
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT paths_from(ARRAY[35, 20, 109], 16, 'edge_type', 1, 'edge_weight', 'FLOAT', 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。
		
		如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。


	
- `backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER,
    IN result_table_name VARCHAR
)`

	从指定节点开始回溯直到根节点的*所有路径*。
	  
	在上图中，从G点开始的所有路径包括：  
	*G-->E-->A*  
	*G-->E-->C-->A*
	
	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT backtrack(ARRAY[35, 20, 109], 16, 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		运行时间依赖于路径的长度，与传入参数中指定的起始节点的数量无关，因此，如果需要回溯多个节点的路径，建议通过数组的方式一次传入多个节点id，而不是每次传入一个节点id、多次调用函数。
		
		如果两个节点之间存在重复的边，则该条重复的边以及路径上后续所有的边，都会重复出现在结果中。如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。
		
		对于只考虑特定类型的边的情形，参见[指定边的类型](#jump2)。


- `backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
)`

	从指定节点开始回溯直到根节点的*所有路径*以及路径上边的属性。

	
	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		attr_column，可变长度字符，指定边表中定义边的属性的字段名称
		
		attr_type，可变长度字符，指定边表中边的属性的类型
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT backtrack(ARRAY[35, 20, 109], 16, 'edge_weight', 'FLOAT', 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		运行时间依赖于路径的长度，与传入参数中指定的起始节点的数量无关，因此，如果需要回溯多个节点的路径，建议通过数组的方式一次传入多个节点id，而不是每次传入一个节点id、多次调用函数。
		
		如果两个节点之间存在重复的边，则该条重复的边以及路径上后续所有的边，都会重复出现在结果中。如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。
		
		目前只支持数值类型的属性，对于长度小于最大长度的路径，返回的属性数组中的最后一个数值无意义（值为NULL），如果长度等于最大路径长度，属性数组中的最后一个数值为路径上最后一条边的属性。
		
		对于只考虑特定类型的边的情形，参见[指定边的类型](#jump4)。



- <span id="jump2">`backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
)`</span>

	从指定节点开始，沿着指定类型的边，直到根节点的*所有路径*。
	
	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称
		
		edge_type_value，整型数值，指定边的类型
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT backtrack(ARRAY[35, 20, 109], 16, 'edge_type', 1, 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。
		
		如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。



- <span id="jump4">`backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
)`</span>

	从指定节点开始，沿着指定类型的边，直到根节点的*所有路径*以及路径上边的属性。
	
	- Parameters
	
		sources，整型数组，指定起始节点的id
		
		max_loops，整型数值，指定路径的最大长度
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称
		
		edge_type_value，整型数值，指定边的类型
		
		attr_column，可变长度字符，指定边表中定义边的属性的字段名称
		
		attr_type，可变长度字符，指定边表中边的属性的类型
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT backtrack(ARRAY[35, 20, 109], 16, 'edge_type', 1, 'edge_weight', 'FLOAT', 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。
		
		如果遍历的节点数量超过了设定值（100,000），则返回当前已遍历到的路径，路径上所包含的节点数量可能超过100,000。
		

- `bfs_from(
    IN source INTEGER, 
    IN result_table_name VARCHAR)`

	从指定节点开始，进行广度优先搜索。
	
	- Parameters
	
		source，整型数值，指定起始节点的id
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的边结果表，属性是边的层级
	
	- Returns
	
		边的数量
	
	- Examples
	
	```
	SELECT bfs_from(20, 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		如果上一层级的多个节点与下一层级某个节点有边连接，那么下一层级上的此节点会在结果中出现多次；在某些场景下，可能需要剔除重复的节点。


- `bfs_from(
    IN source INTEGER,
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR)`

	从指定节点开始，沿着指定类型的边，进行广度优先搜索。
	
	- Parameters
	
		source，整型数值，指定起始节点的id
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称
		
		edge_type_value，整型数值，指定边的类型
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的边结果表，属性是边的层级
	
	- Returns
	
		边的数量
	
	- Examples
	
	```
	SELECT bfs_from(20, 'edge_type', 3, 't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- Notes
	
		如果上一层级的多个节点与下一层级某个节点有边连接，那么下一层级上的此节点会在结果中出现多次；在某些场景下，可能需要剔除重复的节点。


- `meta_path_search(
    IN meta_path_def VARCHAR[],
    IN edge_orientation INTEGER[],
    IN result_table_name VARCHAR)`

	在全图中搜索满足元路径所定义的模式的路径。
	
	- Parameters
	
		meta_path_def，可变字符数组，定义元路径的模式，详细说明参见[Notes](#jump5)
		
		edge_orientation，整型数组，定义边的方向，1代表正向，其他反向
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的路径结果表
	
	- Returns
	
		路径的数量
	
	- Examples
	
	```
	SELECT meta_path_search(
	ARRAY['vertices.node_type = 1 AND vertices.in_degree >= 3','edges.edge_type = 0','vertices.node_type = 0','edges.edge_type = 1','vertices.node_type = 2 AND vertices.in_degree >= 3'],
	ARRAY[0, 1],
	't1');
	SELECT * FROM t1 LIMIT 100;
	```

	- <span id="jump5">Notes</span>
	
		使用可变字符的数组定义元路径，数组中的每个元素是以SQL筛选条件的方式对节点或者边的约束，数组中的第一个和最后一个元素须是对节点的约束，两个节点约束之间穿插边的约束，如果不需要对某个顶点或者某条边进行任何筛选，则传入’1 = 1‘或其他一直为True的约束。
		
		结果表中边的方向需要根据edge_orientation中的配置进行调整，即，如果对应的edge_orientation为1，那么，src_id和dst_id分别为起始点和目的点，如果对应的edge_orientation不为1，此时，src_id和dst_id分别为目的点和起始点。


- `pagerank(
    IN beta REAL, 
    IN delta_min REAL,
    IN max_iter INTEGER,
    IN result_table_name VARCHAR
)`
	 
	pagerank算法。
	
	- Parameters
	
		beta，浮点数值，中断当前访问、跳转到其他页面的概率，e.g.: 0.15
		
		delta_min，浮点数值，每轮迭代时，顶点pagerank值的最小变化量，如果所有的顶点的变化量均小于delta_min，则迭代提前结束，e.g.: (1/number of vertices * 1000)
		
		max_iter，整型数值，指定最大迭代次数
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的顶点结果表，属性为顶点的pagerank值
	
	- Returns
	
		整型数值，运行完成时的迭代次数
	
	- Examples
	
	```sql
	SET optimizer = off;
	SELECT pagerank(.15, .0001, 10, 't1');
	SELECT * FROM t1;
	```

	- Notes
	
		根据madlib代码中备注的描述，在Greenplum 5版本中，当前gporca开启时，算法的效率会显著下降。在Greenplum 6.10.0(PostgreSQL 9.4.24)中测试时，也发现了此现象（图的规模越小，差距越明显，在千万级边的规模上，没有明显差距）。因此，在小图上运行pagerank时，通过`set optimizer = off;`命令临时关闭gporca，以提高算法效率。
		

- `louvain(
    IN max_iter INTEGER,
    IN max_iter_stage1 INTEGER,
    IN max_iter_no_change_stage1 INTEGER,
    IN weight_column VARCHAR,
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
)`

	只考虑指定类型的边，基于louvain算法，对顶点划分社区。

	
	- Parameters
		
		max_iter，最大迭代次数
		
		max_iter_stage1，louvain算法中，stage1步骤的最大迭代次数
		
		max_iter_no_change_stage1，stage1步骤中，如果连续`max_iter_no_change_stage1 `次无顶点社区更新，则退出stage1迭代
		
		weight_column，指定边表中定义边的权重的字段名称，如果为无权重边，则传入NULL
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称，如果不需要考虑边的类型，则传入`NULL`
		
		edge_type_value，整型数值，指定边的类型，如果不需要考虑边的类型，则传入`NULL`
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的顶点结果表，顶点的属性为其社区id

	
	- Returns
	
		无
	
	- Examples
	
	```sql
	SELECT louvain(8, 16, 4, NULL, 'edge_type', 1, 't1');
	SELECT * FROM t1;
	```

	- Notes
		
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。



- `weakly_connected_components(
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
)`

	查找图中所有只包含指定类型边的弱连通分量。

	
	- Parameters
	
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称，如果不需要考虑边的类型，则传入`NULL`
		
		edge_type_value，整型数值，指定边的类型，如果不需要考虑边的类型，则传入`NULL`
		
		result_table_name，可变长度字符，指定结果表名称， 参见[结果表schema](./返回结果说明.md)中的带属性的顶点结果表，顶点的属性为其component_id
	
	- Returns
	
		整型数值，运行完成时的迭代次数
	
	- Examples
	
	```sql
	SELECT weakly_connected_components('edge_type', 1, 't1');
	SELECT * FROM t1;
	```

	- Notes
	
		略。


- `cycle_detect(
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
)`

	查找图中包含在指定类型边的环路上的顶点。

	
	- Parameters
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称，如果不需要考虑边的类型，则传入`NULL`
		
		edge_type_value，整型数值，指定边的类型，如果不需要考虑边的类型，则传入`NULL`
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的顶点结果表

	
	- Returns
	
		无
	
	- Examples
	
	```sql
	SELECT cycle_detect('edge_type', 1, 't1');
	SELECT * FROM t1;
	```

	- Notes
		
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。


- `topo_sorting(
    IN max_loop INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
)`

	只考虑指定类型的边，对图上的顶点进行拓扑排序。。

	
	- Parameters
	
		max_loop，整型数值，最大迭代次数
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称，如果不需要考虑边的类型，则传入`NULL`
		
		edge_type_value，整型数值，指定边的类型，如果不需要考虑边的类型，则传入`NULL`
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的顶点结果表，顶点的属性为其rank

	
	- Returns
	
		整型数值，运行完成时的迭代次数
	
	- Examples
	
	```sql
	SELECT topo_sorting(10, 'edge_type', 1, 't1');
	SELECT * FROM t1;
	```

	- Notes
		
		最大迭代次数`max_loop`小于图中最长路径的长度时，输出的结果不可信。
		
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。


- `triangle_count(
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
)`

	只考虑指定类型的边，对图上的顶点进行拓扑排序。

	
	- Parameters
		
		edge_type_column，可变长度字符，指定边表中定义边的类型的字段名称，如果不需要考虑边的类型，则传入`NULL`
		
		edge_type_value，整型数值，指定边的类型，如果不需要考虑边的类型，则传入`NULL`
		
		result_table_name，可变长度字符，指定结果表的表名，参见[结果表schema](./返回结果说明.md)中的带属性的顶点结果表，顶点的属性为包含该顶点的三角形的数量

	
	- Returns
	
		无
	
	- Examples
	
	```
	SELECT triangle_count('edge_type', 1, 't1');
	SELECT * FROM t1;
	```

	- Notes
		
		边的类型只支持传入一个参数，且进行等值判断；如果需要考虑多种类型的边，可以另外引入一个新的字段，对于需要考虑的类型的边，将该字段设为相同的值。

