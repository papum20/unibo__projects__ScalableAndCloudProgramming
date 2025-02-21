package functions

import org.apache.spark.SparkContext
import util.Util

import java.util.Map
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.HashMap

object MapPairsAggregate {


	/**
	 * from mapCartesianReduce3
	 * OutOfMemory (3GB)
	 * @param sc
	 * @param path_input
	 * @return
	 */
	def mapPairsAggregateConcurrent(sc: SparkContext, path_input: String): Map[(Int, Int), Int] = {

		val in = sc.textFile(path_input)

		// 2. note in groupBy : do an aggregation later (and try to write it then to file)
		// 3. try to write in parallel, w/o first coalesce(1)

		val receipts_to_products = in
			.map { line =>
				val pair = line.split(",")
				(pair(0).toInt, pair(1).toInt)
			}
			.groupBy { pair => pair._1 }

		receipts_to_products
			.flatMap { receipt =>
				Util.getPairs2(
					receipt._2.map { entry => entry._2 }
				)
			}
			.aggregate( new ConcurrentHashMap[(Int,Int),Int]() ) (
				(acc, v) => {
					acc.merge(v._1, v._2, Integer.sum)
					acc
				},
				(map1, map2) => {
					map2.forEach( (key, value) => {
						map1.merge(key, value, Integer.sum) // merge values from different partitions
					})
					map1
				}
			)
	}

	/**
	 * from mapCartesianReduce3
	 * OutOfMemory (3GB)
	 * @param sc
	 * @param path_input
	 * @return
	 */
	def mapPairsAggregate(sc: SparkContext, path_input: String): HashMap[(Int, Int), Int] = {

		val in = sc.textFile(path_input)

		// 2. note in groupBy : do an aggregation later (and try to write it then to file)
		// 3. try to write in parallel, w/o first coalesce(1)

		val receipts_to_products = in
			.map { line =>
				val pair = line.split(",")
				(pair(0).toInt, pair(1).toInt)
			}
			.groupBy { pair => pair._1 }

		receipts_to_products
			.flatMap { receipt =>
				Util.getPairs2(
					receipt._2.map { entry => entry._2 }
				)
			}
			.aggregate( new HashMap[(Int,Int),Int]() ) (
				(acc, e) =>
					acc.+((e._1, acc.getOrElse(e._1, 0) + 1)) ,
				(map1, map2) =>
					map1.merged(map2) {
						(e1, e2) => (e1._1, e1._2 + e2._2)
					}
			)
	}

}
