package functions

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.Util
import util.Util.DEBUG

object MapCartesianReduce {

	def mapCartesianReduce(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		val receipts_to_products = in
			.map { line => line.split(",") }
			.groupBy { pair => pair(0) }

		if (DEBUG) {
			println( "Map:" )
			println( receipts_to_products.count() )
			println( receipts_to_products.take(5)
				.map { receipt => receipt._1 + ": [" +
					receipt._2.map { entry => "(" + entry(0) + "," + entry(1) + ")" }
						.mkString(", ") +
					"]"
				}
				.mkString("\n") )
		}

		val joined = receipts_to_products.map { receipt =>
			Util.getPairs(
				receipt._2.map { entry => entry(1).toInt }
			)
		}

		if (DEBUG) {
			println( "Joined:" )
			println( joined.count() )
			println( joined.take(5).mkString("\n") )
		}

		val counted = joined
			.flatMap { pairs => pairs }
			.groupBy { pair => pair }
			.map { m => (m._1, m._2.size) }

		if (DEBUG) {
			println( "Counted:" )
			println( counted.count() )
			println( counted.take(5).mkString("\n") )
		}

		//val reduced = counted.aggregate( new mutable.HashMap[(Int,Int),Int]() ) (
		//	(acc, receipt) => {
		//		val newAcc = acc.clone()
		//		receipt.foreach { pair =>
		//			newAcc.update( pair, newAcc.getOrElse(pair, 0) + 1 )
		//		}
		//		newAcc
		//	},
		//	(map1, map2) => map1 ++ map2
		//)
		//
		//println( "Reduced:" )
		//println( reduced.size )
		//println( reduced.take(5).mkString("\n") )

		counted.map { entry => entry._1._1 + "," + entry._1._2 + "," + entry._2 }

	}

	/**
	 * first map uses int keys and not strings
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce2(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		val receipts_to_products = in
			.map { line =>
				val pair = line.split(",")
				(pair(0).toInt, pair(1))
			}
			.groupBy { pair => pair._1 }

		val joined = receipts_to_products.map { receipt =>
			Util.getPairs(
				receipt._2.map { entry => entry._2.toInt }
			)
		}

		val counted = joined
			.flatMap { pairs => pairs }
			.groupBy { pair => pair }
			.map { m => (m._1, m._2.size) }

		counted.map { entry => entry._1._1 + "," + entry._1._2 + "," + entry._2 }

	}

	/**
	 * first map uses int values and not strings.
	 * also, group more maps
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce3(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		// 2. note in groupBy : do an aggregation later (and try to write it then to file)

		val receipts_to_products = in
			.map { line =>
				val pair = line.split(",")
				(pair(0).toInt, pair(1).toInt)
			}
			.groupBy { pair => pair._1 }

		receipts_to_products
			.flatMap { receipt =>
				Util.getPairs(
					receipt._2.map { entry => entry._2 }
				)
			}
			.groupBy { pair => pair }
			.map { m => m._1._1 + "," + m._1._2 + "," + m._2.size }

	}

	/**
	 * 2 + 3 +
	 * values' elements are just split[1] instead of whole lines
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce4(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		// 2. note in groupBy : do an aggregation later (and try to write it then to file)

		val receipts_to_products = in
			.map { line =>
				val pair = line.split(",")
				(pair(0).toInt, pair(1).toInt)
			}
			.groupBy { pair => pair._1 }
			.mapValues { entries => entries.map { entry => entry._2 } }

		receipts_to_products
			.flatMap { receipt =>
				Util.getPairs(
					receipt._2
				)
			}
			.groupBy { pair => pair }
			.map { m => m._1._1 + "," + m._1._2 + "," + m._2.size }

	}

	/**
	 * 3 + getPairs2 (also returns sizes, and group by key)
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce5(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		val receipts_to_products = in
			.map { line =>
				val pair = line.split(",")
				(pair(0).toInt, pair(1).toInt)
			}
			.groupBy { pair => pair._1 }

		val pairs = receipts_to_products
			.flatMap { receipt =>
				Util.getPairs2(
					receipt._2.map { entry => entry._2 }
				)
			}

		pairs
			.groupBy { pair => pair }
			.map { m => m._1._1 + "," + m._1._2 + "," + m._2.size }

	}

	/**
	 * 5 + simplified + groupByKey
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce6(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

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
			.groupByKey()
			.map { m => m._1._1 + "," + m._1._2 + "," + m._2.size }

	}

	/**
	 * 6 +
	 * reduceByKey.
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce7(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

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
			.reduceByKey { (a, b) => a + b }
			.map { m => m._1._1 + "," + m._1._2 + "," + m._2 }

	}

	/**
	 * 7 +
	 * don't return strings.
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce8(sc: SparkContext, path_input: String): RDD[((Int, Int), Int)] = {

		val in = sc.textFile(path_input)

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
			.reduceByKey { (a, b) => a + b }

	}

	/**
	 * 7 +
	 * also use groupByKey first.
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce9(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		val receipts_to_products = in
			.map { line =>
				val pair = line.split(",")
				(pair(0).toInt, pair(1).toInt)
			}
			.groupByKey()

		receipts_to_products
			.flatMap { receipt =>
				Util.getPairs2( receipt._2 )
			}
			.reduceByKey { (a, b) => a + b }
			.map { m => m._1._1 + "," + m._1._2 + "," + m._2 }

	}

	/**
	 * 9 + pattern match case
	 * @param spark
	 * @param path_input
	 * @return
	 */
	def mapCartesianReduce10(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		val receipts_to_products = in
			.map { line =>
				line.split(",") match {
					case pair: Array[String] => (pair(0).toInt, pair(1).toInt)
				}
			}
			.groupByKey()

		receipts_to_products
			.flatMap { receipt =>
				Util.getPairs2( receipt._2 )
			}
			.reduceByKey { (a, b) => a + b }
			.map {
				case ((a: Int, b: Int), count: Int) => a + "," + b + "," + count
			}

	}

}
