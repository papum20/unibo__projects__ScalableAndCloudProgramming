package functions

import util.Util.getPairs
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.util.concurrent.ConcurrentHashMap

object MapCartesianConcurrentHashMap {

	/**
	 * OutOfMemory (3GB)
	 * @param path_input
	 * @param path_output
	 */
	private def mapCartesian_concurrentHashMap(path_input: String, path_output: String): Unit = {

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val in = sc.textFile(path_input)

		// should I first map?
		// is it better if elements are just split[0] instead of whole lines?

		val receipts_to_products = in
			.map { line => line.split(",") }
			.groupBy { pair => pair(0) }

		println( "Map:" )
		println( receipts_to_products.count() )
		println( receipts_to_products.take(5)
			.map { receipt => receipt._1 + ": [" +
				receipt._2.map { entry => "(" + entry(0) + "," + entry(1) + ")" }
					.mkString(", ") +
				"]"
			}
			.mkString("\n") )

		val joined = receipts_to_products.map { receipt =>
			getPairs(
				receipt._2.map { entry => entry(1).toInt }
			)
		}

		println( "Joined:" )
		println( joined.count() )
		println( joined.take(5).mkString("\n") )
		println( joined.take(5).mkString("\n") )

		// 1. concurrent hash map out of memory (3g)
		// 2. even non-concurrent, and cloning each time, is too slow, and not parallel (3/6g)
		val reduced = joined.aggregate( new ConcurrentHashMap[(Int,Int),Int]() ) (
			(acc, receipt) => {
				receipt.foreach { entry =>
					acc.merge(entry, 1, Integer.sum)
				}
				acc
			},
			(map1, map2) => {
				map2.forEach((key, value) => {
					map1.merge(key, value, Integer.sum) // merge values from different partitions
				})
				map1
			}
		)

		println( "Reduced:" )
		println( reduced.size )
		println( reduced.entrySet().toArray().toList.take(5).mkString("\n") )
	}
}
