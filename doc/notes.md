# NOTES

## Evaluation

Strong scalability : given an input, times reduce when adding more resources  
*	e.g.:
	*	start with 1 worker, measure wall clock time
	*	try with 2 workers : if time halved, strong scalability at max, if time the same, nothing, usually in the middle
	*	increase... (max 4, with google cloud plan)

weak scalability : while adding resources, also increase work load (i.e. increase dataset size)  
*	e.g.: double resources, double input size
	*	measure : expect time to be the same
		*	usually takes (a little) longer

wall clock time : physical time from start to end of a task  


## code

e.g.: PageRank sol :
*	`val conf = ` ... `.setMaster("local[*]")` : run in parallel on local machine, using all available cores  
*	`.setMaster("local[2]")` : use 2 cores  


## implementation

(local, 1 worker, 16 cores) map to split before groupBy is much faster (<0.5s) than just groupBy with split inside (2s) (old tests, maybe not valid, with `splitBefore`, now removed)  
*	`map{split(",")}.groupBy{pair[0]}`
*	`groupBy{split(",")[0]}`

`countByValue()` : is not scalable
*	Return the count of each unique value in this RDD as a local map of (value, count) pairs.
*	This method should only be used if the resulting map is expected to be small, as the whole thing is loaded into the driver's memory.
*	To handle very large results, consider using `rdd.map(x => (x, 1L)).reduceByKey(_ + _)`, which returns an RDD[T, Long] instead of a map.

`aggregate()` (or `reduce`) (to convert to map or concurrent map) too not scalable, as doesn't return an RDD  

use HDFS functions, instead of std java IO, just for consistency and for resilience and distribution with future improvements  
(even though it's not needed, at the moment, for such simple tasks)

write output :
*	use spark's save (and not collect first), to make scalable  
*	distributed write and then fileOutputStream copy, and not first coalesce to create single file, it's faster
	*	test: local, ca. 200s (190s RDD to files, 10s copy all files into one) vs 290s

## performance

2 workers :  

removed local[]; SparkSession  
3  
```
2025-01-29 14:02:44
mapCartesianReduce3 (ms): MutableList(1028)
2025-01-29 14:22:10
mapCartesianReduce3_write (ms): MutableList(1164313)
```
9  
```
2025-01-29 14:36:48
mapCartesianReduce_groupByKey_reduceByKey (ms): MutableList(1114)
2025-01-29 14:47:12
mapCartesianReduce_groupByKey_reduceByKey_write (ms): MutableList(647445)
```
SparkContext  
10  
```
2025-01-29 15:11:17
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(891)
2025-01-29 15:22:09
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(651140)
```
6
```
2025-01-29 15:35:36
mapCartesianReduce_groupByKey (ms): MutableList(901)
2025-01-29 15:48:44
mapCartesianReduce_groupByKey_write (ms): MutableList(786762)
```
3 workers :  
1
```
25/02/17 12:21:44
mapCartesianReduce1 (ms): MutableList(772)
25/02/17 12:36:17
mapCartesianReduce1_write (ms): MutableList(873535)
```
2
```
25/02/17 12:04:42
mapCartesianReduce2 (ms): MutableList(817)
25/02/17 12:20:29
mapCartesianReduce2_write (ms): MutableList(947174)
```
3
```
25/02/17 13:54:13
mapCartesianReduce3 (ms): MutableList(1026)
25/02/17 14:17:50
mapCartesianReduce3_write (ms): MutableList(1416774)
```
4
```
25/02/17 10:14:44
mapCartesianReduce4 (ms): MutableList(763)
25/02/17 10:38:15
mapCartesianReduce4_write (ms): MutableList(1410627)
```
5
```
5/02/17 11:41:43
mapCartesianReduce5 (ms): MutableList(921)
25/02/17 12:03:40
mapCartesianReduce5_write (ms): MutableList(1317293)
```
6
```
25/02/14 16:14:10
mapCartesianReduce_groupByKey (ms): MutableList(763)
25/02/14 16:23:41
mapCartesianReduce_groupByKey_write (ms): MutableList(571140)
```
7
```
2025-01-31 15:46:58 
mapCartesianReduce_reduceByKey (ms): MutableList(1127)
2025-01-31 15:56:12
mapCartesianReduce_reduceByKey_write (ms): MutableList(553044)
```
8
```
25/01/31 16:26:44
mapCartesianReduce_reduceByKey_noString (ms): MutableList(754)
25/01/31 16:37:20
mapCartesianReduce_reduceByKey_noString_write (ms): MutableList(636282)
```
9
```
25/02/14 14:47:06
mapCartesianReduce_groupByKey_reduceByKey (ms): MutableList(2512)
25/02/14 14:58:19
mapCartesianReduce_groupByKey_reduceByKey_write (ms): MutableList(673260)
```
10
```
25/02/14 15:04:54
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(765)
25/02/14 15:17:02
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(728371)
```
retry, to choose final, 7 9 10  
7
```
25/02/19 11:55:48
mapCartesianReduce_reduceByKey (ms): MutableList(1823)
25/02/19 12:11:24
mapCartesianReduce_reduceByKey_write (ms): MutableList(935760)
```
9
```
25/02/19 12:15:19
mapCartesianReduce_groupByKey_reduceByKey (ms): MutableList(802)
25/02/19 12:27:55
mapCartesianReduce_groupByKey_reduceByKey_write (ms): MutableList(756017)
```
10
```
25/02/19 12:43:06
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(791)
25/02/19 12:52:18
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(551890)
```
10
```
25/02/20 09:11:43
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(771)
25/02/20 09:20:39 
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(536158)
```
7
```
```
9
```
```
4
```
```
5
```
```

1 workers:  
spark, local*  
10
```
25/02/20 11:07:29
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(1930)
25/02/20 11:26:57
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(1168303)
```
sc, local*  
10
```
25/02/20 12:07:48
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(1317)
25/02/20 12:32:53
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(1505099)
```

w/ 1 worker, need `local[*]`, or error
```
ERROR SparkHadoopWriter: Aborting job job_202502201021173750682272632526358_0007.
org.apache.spark.SparkException: Job 0 cancelled because SparkContext was shut down
```
local allows local mode, so use local cpu threads as executors, but still single node, i.e. single JVM

10
local *
Launching: mapCartesianReduce_groupByKey_reduceByKey_match
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(861)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(129965)
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(861)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(129965)

local 1
Launching: mapCartesianReduce_groupByKey_reduceByKey_match
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(330)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(367543)
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(330)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(367543)

local 8
Launching: mapCartesianReduce_groupByKey_reduceByKey_match
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(295)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(110201)
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(295)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(110201)

local 16
Launching: mapCartesianReduce_groupByKey_reduceByKey_match
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(258)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(122224)
mapCartesianReduce_groupByKey_reduceByKey_match (ms): MutableList(258)
mapCartesianReduce_groupByKey_reduceByKey_match_write (ms): MutableList(122224)
