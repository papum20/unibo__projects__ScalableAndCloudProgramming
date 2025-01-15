package functions

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.Util.getPairs

import java.io.File
import scala.collection.mutable

object MapCartesianReduce {

	val DEBUG = false

	def mapCartesianReduce(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		// should I first map?
		// is it better if elements are just split[0] instead of whole lines?

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
			getPairs(
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



}
