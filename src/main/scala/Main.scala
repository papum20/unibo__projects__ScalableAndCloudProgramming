import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Main {

	private val PATH_DATASET_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/src/main/resources/order_products.csv"

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("pageRank").setMaster("local[*]")
		val sc = new SparkContext(conf)

		//val in = Source.fromFile(PATH_DATASET_LOCAL)
		//val word = in.getLines.toList filter (w => w forall (c => c.isLetter))

		println("Hello")
	}
}