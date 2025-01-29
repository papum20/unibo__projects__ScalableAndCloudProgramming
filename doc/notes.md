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

(local, 1 worker, 16 cores) map to split before groupBy is much faster (<0.5s) than just groupBy with split inside (2s)  
*	`map{split(",")}.groupBy{pair[0]}`
*	`groupBy{split(",")[0]}`

`countByValue()` : is not scalable
*	Return the count of each unique value in this RDD as a local map of (value, count) pairs.
*	This method should only be used if the resulting map is expected to be small, as the whole thing is loaded into the driver's memory.
*	To handle very large results, consider using `rdd.map(x => (x, 1L)).reduceByKey(_ + _)`, which returns an RDD[T, Long] instead of a map.

`aggregate()` too not scalable, as doesn't return an RDD  

use HDFS functions, instead of std java IO, just for consistency and for resilience and distribution with future improvements  
(even though it's not needed, at the moment, for such simple tasks)

write output :
*	use spark's save (and not collect first), to make scalable  
*	distributed write and then fileOutputStream copy, and not first coalesce to create single file, it's faster
	*	test: local, ca. 200s (190s RDD to files, 10s copy all files into one) vs 290s

## performance

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
7
```
```
