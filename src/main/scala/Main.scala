import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.io.Source

object Main {

	private val PATH_DATASET_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/src/main/resources/order_products.csv"

	private val PATH_OUTPUT_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/out/output.csv"


	def main(args: Array[String]): Unit = {

		printTime( mapJoin(PATH_DATASET_LOCAL, PATH_OUTPUT_LOCAL) )
		//printTime(test1(PATH_DATASET_LOCAL, PATH_OUTPUT_LOCAL))
		//printTime(test2(PATH_DATASET_LOCAL, PATH_OUTPUT_LOCAL))

		println("Hello")
	}


	private def getPairs(elems: Iterable[Int]): Iterable[(Int, Int)] =
		for {
			a <- elems
			b <- elems
			// 1. avoid comparison when they're equal
			// 2. don't repeat pairs
			// 3. compute fewer pairs
			if a < b
		} yield (a,b)

	private def mapJoin(path_input: String, path_output: String): Unit = {

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

		//val reduced = joined.aggregate( Hash,
		//	(acc, entry) => {
		//
		//	},
		//	(a, b) => {
		//
		//	}
		//)

		FileUtils.deleteDirectory(new File(path_output))
		//ranks.saveAsTextFile(outputFile)
		sc.stop()
	}


	private def productsCorrelations(path_input: String, path_output: String): Unit = {

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val in = sc.textFile(path_input)

		// should I first map?
		// is it better if elements are just split[0] instead of whole lines?

		val receipts_to_products = in.groupBy { line =>
			line.split(",")(0)
		}

		println( "Map:" )
		println( receipts_to_products.count() )
		println( receipts_to_products.take(5).mkString("\n") )

		val joined = receipts_to_products.join(receipts_to_products)

		println( "Joined:" )
		println( joined.count() )
		println( joined.take(5).mkString("\n") )

		FileUtils.deleteDirectory(new File(path_output))
		//ranks.saveAsTextFile(outputFile)
		sc.stop()
	}

	private def test1(path_input: String, path_output: String): Unit = {

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val in = sc.textFile(path_input)

		// is it better if elements are just split[0] instead of whole lines?

		val receipts_to_products = in.groupBy { line =>
			line.split(",")(0)
		}

		FileUtils.deleteDirectory(new File(path_output))
		sc.stop()
	}

	private def test2(path_input: String, path_output: String): Unit = {

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val in = sc.textFile(path_input)

		// is it better if elements are just split[0] instead of whole lines?

		val receipts_to_products = in
			.map { line => line.split(",") }
			.groupBy { pair => pair(0) }

		FileUtils.deleteDirectory(new File(path_output))
		sc.stop()
	}


	/**
	 * Evaluate an expression and print the elapsed time.
	 */
	private def printTime[R](block: => R) = {
		val t0 = System.nanoTime()
		val result = block    // call-by-name
		val t1 = System.nanoTime()
		println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
		result
	}

	private def printMem(): Unit = {

		val runtime = Runtime.getRuntime
		println(
			"max: "		+ runtime.maxMemory()	+ "\n" +
			"free: "	+ runtime.freeMemory()	+ "\n" +
			"total: "	+ runtime.totalMemory()	+ "\n"
		)
	}

}