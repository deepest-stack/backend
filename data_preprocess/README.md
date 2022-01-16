# 数据预处理 & 特征加工


## 缺失值处理
- 忽略该行
- 替换为指定值/最高频值(most_common)
	- `most_common(
    IN table_name VARCHAR, 
    IN column_name VARCHAR,
    IN dummy anyelement
)`
	
		查找表中某个字段的最高频值
		
		- Parameters
		
			`table_name `，可变长度字符，指定表名
			
			`column_name `，可变长度字符，知道字段名
			
			`dummy`，与制度字段类型相同的任意值
		
		- Returns
		
			与`dummy`/指定字段同类型，字段的最高频值
			
		- Examples
			
			```
			psql>SELECT most_common('edges', 'src_id', 1::INTEGER);
			 most_common
			-------------
    		   63247
			(1 row)
			```
		
		- Notes
			
			`::TYPE`非必需
		

- 替换为最大值(max)/最小值(min)/平均值(avg)/中位数(median)（仅限于数值型）


## 数值型

- 单个特征/多个特征的四则运算(+,-,*,/)、模运算(%)、幂运算(^)、指数运算(exp,power)、对数运算(ln, log)
- 标准化，转换为标准正态分布，(x-avg)/stddev；
- L1归一化，只限于正值数据，x/sum(x)；
- 0-1规范化，区间缩放(x-min)/(max-min)；
- 二值化，binaryze(x, theshold, p, n) 
	- `binaryze(
    IN x NUMERIC, 
    IN threshold NUMERIC,
    IN p INTEGER,
    IN n INTEGER
)`
	
		对连续型变量进行二值化处理
		
		- Parameters
		
			`x`，连续型数值变量
			
			`threshold `，数值类型，二分的阈值
			
			`p`，整型数值，当`x>=threshold`时，返回`p`
			
			`n`，整型数值，当`x<threshold`时，返回`n`
			
		- Returns
		
			整型数值，二值化处理的结果
			
		- Examples
		
			```
			psql>SELECT src_id,  binaryze(src_id::NUMERIC, 3::NUMERIC, 1, 0) FROM edges LIMIT 10;
			 src_id | binaryze
			 -------+----------
			      0 |        0
			      0 |        0
			     35 |        1
			     35 |        1
			     47 |        1
			     50 |        1
			     64 |        1
			     78 |        1
			     78 |        1
			     79 |        1
			(10 rows)
			```
			
		- Notes
		
			`::TYPE`非必需


## 分类型
- one-hot编码，枚举值的选取方法：a）手动指定，b）topN，c）top 95%；

	- `top_n(
    IN table_name VARCHAR, 
    IN column_name VARCHAR,
    IN dummy anyelement,
    IN n INTEGER
)`

		返回表中某个字段出现频率最高的`n`个值
		
		- Parameters
		
			- `table_name`，可变长度字符，指定表名
			
			- `column_name`，可变长度字符，指定字段名
			
			- `dummy`，与字段类型相同的任意值
			
			- `n`，指定频率最高的值的个数
		
		- Returns
		
			与`dummy`/指定字段同类型的数组，频率最高的至多前`n`个值
			
		- Eamples
		
			```
			psql>SELECT top_n('edges', 'src_id', 1::INTEGER, 10);
						      top_n
			-----------------------------------------------------------------
			 {105861,43350,294743,5713,17508,38331,46470,392420,37845,39333}
			(1 row)
			```
		
		- Notes
	
			如果不足`n`个，则全部返回；
			
			如果频率排名前`n`的值超过`n`个，则只返回`n`个，且返回结果不确定，每次的返回结果可能不同。
	
	- `top_percent(
    IN table_name VARCHAR, 
    IN column_name VARCHAR,
    IN dummy anyelement,
    IN percent REAL
)`

		返回表中某个字段累计出现频率占比超过`percent`的前几个值
		
		- Parameters
		
			- `table_name`，可变长度字符，指定表名
			
			- `column_name`，可变长度字符，指定字段名
			
			- `dummy`，与字段类型相同的任意值
			
			- `percent`，0~1之间的浮点数，指定累计出现频率占比
		
		- Returns
		
			与`dummy`/指定字段同类型的数组，累计出现频率占比超过`percent`的前几个值
			
		- Eamples
		
			```
			psql>SELECT top_percent('edges', 'src_id', 1::INTEGER, 0.00001);
			  top_percent
			----------------
			 {366515,27022}
			```
			
		- Notes
			
			如果前几个值中，有出现频率相同的值，且返回结果不确定，每次的返回结果可能不同。
	
	- `onehot_encode(
    IN x anyelement,
    IN enum anyarray,
    IN with_others INTEGER
)`

		对分类型变量进行one-hot编码
		
		- Parameters
		
			- `x`，任意类型，待编码的变量
			
			- `enum`，与`x`相同类型的数组，`x`的枚举值列表
			
			- `with_others`，整型数值，`1`表示在结果中追加`others`列（编码的长度会增加1），其他值表示不追加
		
		- Returns
		
			整型数组，变量的one-hot编码结果
			
		- Eamples
		
			```
			psql>SELECT src_id, onehot_encode(src_id, ARRAY[0,3,1], 1) FROM edges limit 10;
			 src_id | onehot_encode
			 -------+---------------
			      1 | {0,0,1,0}
			     13 | {0,0,0,1}
			     13 | {0,0,0,1}
			     19 | {0,0,0,1}
			     19 | {0,0,0,1}
			     19 | {0,0,0,1}
			     26 | {0,0,0,1}
			     30 | {0,0,0,1}
			     30 | {0,0,0,1}
			     36 | {0,0,0,1}
			(10 rows)
			psql>SELECT src_id, onehot_encode(src_id, ARRAY[0,3,1], 0) FROM edges limit 10;
			 src_id | onehot_encode
			 -------+---------------
			      1 | {0,0,1}
			     13 | {0,0,0}
			     13 | {0,0,0}
			     19 | {0,0,0}
			     19 | {0,0,0}
			     19 | {0,0,0}
			     26 | {0,0,0}
			     30 | {0,0,0}
			     30 | {0,0,0}
			     36 | {0,0,0}
			(10 rows)
			```
		
		
		
		- Notes
		
			如果`x`没有出现在枚举值列表`enum`中，则把`others`列置为1；如果没有追加`others`列（即，`with_others`不为1），则会返回一个全零的编码。
	
	
	- 手动指定枚举值列表，见`onehot_encode`
	
	- topN
	
		```
		psql>CREATE TEMP TABLE enum AS SELECT top_n('edges', 'src_id', 1::INTEGER, 10) DISTRIBUTED RANDOMLY;
		psql>SELECT * FROM enum;
					      top_n
		------------------------------------------------------------------
		 {248506,6131,128409,125271,76361,110152,383044,58412,8060,40048}
		(1 row)
		psql>SELECT src_id, onehot_encode(src_id, top_n, 1) FROM enum, edges LIMIT 10;
		 src_id |      onehot_encode
		 -------+-------------------------
		      5 | {0,0,0,0,0,0,0,0,0,0,1}
		      5 | {0,0,0,0,0,0,0,0,0,0,1}
		      5 | {0,0,0,0,0,0,0,0,0,0,1}
		     21 | {0,0,0,0,0,0,0,0,0,0,1}
		     52 | {0,0,0,0,0,0,0,0,0,0,1}
		     56 | {0,0,0,0,0,0,0,0,0,0,1}
		     62 | {0,0,0,0,0,0,0,0,0,0,1}
		    110 | {0,0,0,0,0,0,0,0,0,0,1}
		    111 | {0,0,0,0,0,0,0,0,0,0,1}
		    123 | {0,0,0,0,0,0,0,0,0,0,1}
		(10 rows)
		```

		
	- top %
	
		```
		psql>CREATE TEMP TABLE enum AS SELECT top_percent('edges', 'src_id', 1::INTEGER, 0.00003) DISTRIBUTED RANDOMLY;
		psql>SELECT * FROM enum;
			 top_percent
		-----------------------------
		 {115184,366515,88780,27022}
		(1 row)
		psql>SELECT src_id, onehot_encode(src_id, top_percent, 1) FROM enum, edges LIMIT 10;
		 src_id | onehot_encode
		 -------+---------------
		      5 | {0,0,0,0,1}
		      5 | {0,0,0,0,1}
		      5 | {0,0,0,0,1}
		     21 | {0,0,0,0,1}
		     52 | {0,0,0,0,1}
		     56 | {0,0,0,0,1}
		     62 | {0,0,0,0,1}
		    110 | {0,0,0,0,1}
		    111 | {0,0,0,0,1}
		    123 | {0,0,0,0,1}
		(10 rows)
		```
		
		
		
