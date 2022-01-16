## performance benchmark


|数据集|图算法|GP SQL|MADlib|graphX|备注|
|:---:|:---:|:---:|:---:|:---:|:---:|
|enron(36698顶点，183839边)|pagerank|2444.985 ms/1211.713 ms|1993.228 ms|4220 ms|GP SQL为分别在gporca开启/关闭下的测试结果，madlib执行pagerank时会关闭gporca|
|~|bfs|476.258 ms/194.389 ms|2193.797 ms/844.569 ms|/|gporca开启/关闭|
|~|weakly connected components|2485.144 ms/1 1035.034 ms|3819 ms/1756 ms|757 ms|gporca开启/关闭|
|~|single source shortest path|1343.850 ms/927.063 ms|371.574 ms/212.730 ms|/|gporca开启/关闭|
|~|topological sorting|1612 ms/963 ms|/|2158 ms|最长路径长度超过1024，没有在限定时间内运行完，因此，统计前10轮迭代的总运行时间（理论上，越往后，单轮迭代时间越短）|
|~|triangle count| 4748.575 ms/ 3818.266 ms|/|2815 ms|gporca开启/关闭|
|~|cycle detect|12498.542 ms /3000.980 ms |/|19135 ms|gporca开启/关闭|
|~|community detect(louvain)|50776.324 ms/ 14405.645 ms|/|92233 ms|gporca开启/关闭;原始图，367662边，环路未剔除|
|amazon(403318顶点，1140070边)|pagerank|3023.688 ms/3230.992 ms|3748.060 ms|13127 ms|GP SQL为分别在gporca开启/关闭下的测试结果，madlib执行pagerank时会关闭gporca|
|~|bfs|329.530 ms/504.836 ms|24723.531 ms/16848.788 ms|/|gporca开启/关闭|
|~|weakly connected components|31998.644 ms / 29017.664 ms|38058.366 ms/34831.052 ms|10395 ms|gporca开启/关闭|
|~|single source shortest path|424.988 ms/596.317 ms|1193.451 ms/844.186 ms|/|gporca开启/关闭|
|~|topological sorting|65299 ms/64974 ms|/|77390 ms|gporca开启/关闭；最长路径长度193|
|~|triangle count|6179.778 ms/ 5048.211 ms |/|6471 ms|gporca开启/关闭|
|~|cycle detect|60268.903 ms/ 43514.184 ms|/|97860 ms|gporca开启/关闭|
|~|community detect(louvain)|117524.758 ms / 69162.631 ms|/|571570 ms|gporca开启/关闭;原始图，3387388边，环路未剔除|
|live journal(3997962顶点，34681189边)|pagerank|86743.546 ms/81097.193 ms|92182.272 ms|119029 ms|GP SQL为分别在gporca开启/关闭下的测试结果，madlib执行pagerank时会关闭gporca|
|~|bfs|368.499 ms/12540.680ms|> 100,000 ms|/|GP SQL为分别在gporca开启/关闭下的测试结果，gporca是否开启对madlib的效率没有显著影响|
|~|weakly connected components|112604.776 ms/112439.704 ms|211898 ms/204133 ms|35965 ms|gporca开启/关闭|
|~|single source shortest path|460.408 ms/14217.599 ms|12351.317 ms/11700.675 ms|/|gporca开启/关闭|
|~|topological sorting| 86673 ms/73085 ms|/|51689 ms|gporca开启/关闭；没有在限定时间内运行完，因此，统计前10轮迭代的总运行时间（理论上，越往后，单轮迭代时间越短）|
|~|triangle count|19min47.217s /42min37.184s |/|OOM(24G)|gporca开启/关闭|
|~|cycle detect|1h44min54.636s /> 3h |/|OOM(24G)|gporca开启/关闭|
|~|community detect(louvain)|26min57.503s / 26min49.011s|/|未在可接受时间内运行完成|gporca开启/关闭|


- 测试环境

	- CPU: Intel(R) Core(TM) i7-5820K CPU @ 3.30GHz(6C 12T)
	- 内存: 32G
	- 硬盘: 512G SSD
	- 操作系统: Ubuntu 16.04.7 LTS 
	- Greenplum: 6.10.0(PostgreSQL 9.4.24), 9 segments locate in 3 Docker containers on same host
	- MADlib: MADlib 1.17.0
	- graphX: spark 3.0.1, local mode

- 测试说明

	- graphX的运行时间均不包括加载文件的时间

	- pagerank：GP SQL功能测试，结果与madlib一致
	
	- weakly connected components：GP SQL功能测试，结果与madlib一致

	- bfs：随机从图中选取10个顶点，分别从这些顶点执行bfs操作，计算平均运行时间

	- single source shortest path：随机从图中选取10个顶点，分别搜索这些顶点到可达顶点的最短路径，计算平均运行时间
	
	- cycle detect：原图中无环路，因此，在图中手动添加一条边，构成环路
	
- 其他说明

	- 在小数据量下，开启gporca会降低性能。

	- madlib的bfs效率低，在每一轮迭代时，会从边中创建一张临时的边表（剔除已访问到的顶点），然后基于此临时表关联下一层的顶点，该临时表无索引，join效率低.
	
	- 在测试GP SQL的single source shortest path算法时，观察到这样的现象：关闭gporca，执行函数，然后打开gporca，执行函数，效率没有提升；关闭当前session，打开一个新的session，并开启gporca，执行函数，效率显著提升。怀疑是执行计划缓存导致（single source shortest path中均为静态SQL）。

	- graphX在weakly connected components上优势明显，wcc每一轮迭代只会更新部分顶点的component_id，在这种情况下，内存操作可能更有优势。


- 测试脚本

	- 通用脚本
	
	```
	-- pagerank
	-- GP SQL
	drop table if exists t1; select pagerank(.15, .00001, 16, 't1');
	-- MADlib
	drop table if exists pr, pr_summary; select madlib.pagerank('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id','pr', null, 16, .00001);
			
	--  weakly connected components
	select weakly_connected_components('wcc_t');
	drop table if exists wcc_t, wcc_t_summary; select madlib.weakly_connected_components('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id','wcc_t');
	```

	- enron
	
	```
	-- bfs
	-- GP SQL
	select now();
	select bfs_from(1092, 't11');
	select bfs_from(18267, 't12');
	select bfs_from(492, 't13');
	select bfs_from(8314, 't14');
	select bfs_from(9297, 't15');
	select bfs_from(8683, 't16');
	select bfs_from(16105, 't17');
	select bfs_from(5835, 't18');
	select bfs_from(18723, 't19');
	select bfs_from(17407, 't20');
	select now();
	-- MADlib
	select now();
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',1092, 't11');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',18267, 't12');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',492, 't13');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',8314, 't14');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',9297, 't15');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',8683, 't16');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',16105, 't17');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',5835, 't18');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',18723, 't19');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',17407, 't20');
	select now();
	
	-- single source shortest path
	-- GP SQL
	select now();
	select shortest_path(1092, -1, 256, 't11');
	select shortest_path(18267, -1, 256, 't12');
	select shortest_path(492, -1, 256, 't13');
	select shortest_path(8314, -1, 256, 't14');
	select shortest_path(9297, -1, 256, 't15');
	select shortest_path(8683, -1, 256, 't16');
	select shortest_path(16105, -1, 256, 't17');
	select shortest_path(5835, -1, 256, 't18');
	select shortest_path(18723, -1, 256, 't19');
	select shortest_path(17407, -1, 256, 't20');
	select now();
	-- MADlib
	select now();
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',1092, 't11');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',18267, 't12');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',492, 't13');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',8314, 't14');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',9297, 't15');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',8683, 't16');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',16105, 't17');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',5835, 't18');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',18723, 't19');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',17407, 't20');
	select now();
	```
	
	- amazon
	
	```
	-- bfs
	-- GP SQL
	select now();
	select bfs_from(   2479,'t11');
	select bfs_from( 133995,'t12');
	select bfs_from( 154499,'t13');
	select bfs_from( 211424,'t14');
	select bfs_from( 226298,'t15');
	select bfs_from( 257078,'t16');
	select bfs_from( 335224,'t17');
	select bfs_from( 339389,'t18');
	select bfs_from( 361343,'t19');
	select bfs_from( 364176,'t20');
	select now();
	-- MADlib
	select now();
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',   2479,'t11');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 133995,'t12');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 154499,'t13');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 211424,'t14');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 226298,'t15');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 257078,'t16');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 335224,'t17');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 339389,'t18');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 361343,'t19');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 364176,'t20');
	select now();
	
	-- single source shortest path
	-- GP SQL
	select now();
	select shortest_path(   2479,-1, 256, 't11');
	select shortest_path( 133995,-1, 256, 't12');
	select shortest_path( 154499,-1, 256, 't13');
	select shortest_path( 211424,-1, 256, 't14');
	select shortest_path( 226298,-1, 256, 't15');
	select shortest_path( 257078,-1, 256, 't16');
	select shortest_path( 335224,-1, 256, 't17');
	select shortest_path( 339389,-1, 256, 't18');
	select shortest_path( 361343,-1, 256, 't19');
	select shortest_path( 364176,-1, 256, 't20');
	select now();
	-- MADlib
	select now();
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',   2479,'t11');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 133995,'t12');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 154499,'t13');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 211424,'t14');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 226298,'t15');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 257078,'t16');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 335224,'t17');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 339389,'t18');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 361343,'t19');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id', 364176,'t20');
	select now();
	```
	
	- live journal
	
	```
	-- bfs
	-- GP SQL
	select now();
	select bfs_from( 3812319, 't11');
	select bfs_from( 2447577, 't12');
	select bfs_from( 1389363, 't13');
	select bfs_from( 1174693, 't14');
	select bfs_from( 3548065, 't15');
	select bfs_from( 2813446, 't17');
	select bfs_from( 2177989, 't18');
	select bfs_from( 1066494, 't19');
	select bfs_from( 1713588, 't20');
	select now();
	-- MADlib
	select now();
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  3812319, 't11');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  2447577, 't12');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1389363, 't13');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1174693, 't14');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  3548065, 't15');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  2813446, 't17');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  2177989, 't18');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1066494, 't19');
	select madlib.graph_bfs('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1713588, 't20');
	select now();
	
	-- single source shortest path
	-- GP SQL
	select now();
	select shortest_path( 3812319, -1, 256, 't11');
	select shortest_path( 2447577, -1, 256, 't12');
	select shortest_path( 1389363, -1, 256, 't13');
	select shortest_path( 1174693, -1, 256, 't14');
	select shortest_path( 3548065, -1, 256, 't15');
	select shortest_path( 2813446, -1, 256, 't17');
	select shortest_path( 2177989, -1, 256, 't18');
	select shortest_path( 1066494, -1, 256, 't19');
	select shortest_path( 1713588, -1, 256, 't20');
	select now();
	-- MADlib
	select now();
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  3812319, 't11');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  2447577, 't12');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1389363, 't13');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1174693, 't14');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  3548065, 't15');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  2813446, 't17');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  2177989, 't18');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1066494, 't19');
	select madlib.graph_sssp('vertices', 'vid', 'edges', 'src=src_id, dest=dst_id',  1713588, 't20');
	select now();
	```
	