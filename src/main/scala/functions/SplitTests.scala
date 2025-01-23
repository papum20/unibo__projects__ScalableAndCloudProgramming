package functions

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SplitTests {

	def splitInside(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		// is it better if elements are just split[0] instead of whole lines?

		val receipts_to_products = in.groupBy { line =>
			line.split(",")(0)
		}

		receipts_to_products.flatMap { receipt =>
			receipt._2.map { entry => entry }
		}
	}

	def splitBefore(sc: SparkContext, path_input: String): RDD[String] = {

		val in = sc.textFile(path_input)

		// is it better if elements are just split[0] instead of whole lines?

		val receipts_to_products = in
			.map { line => line.split(",") }
			.groupBy { pair => pair(0) }


		receipts_to_products.flatMap { receipt =>
			receipt._2.map { entry => entry.toString }
		}
	}

}
