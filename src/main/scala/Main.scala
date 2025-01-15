import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import functions.{MapCartesianReduce, SplitTests}
import util.Util
import util.Time

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.io.Source

object Main {

	private val IS_LOCAL = true

	private val DATASET_PATH_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/src/main/resources/order_products.csv"
	private val OUTPUT_DIR_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/out/"
	private val DATASET_PATH_REMOTE =
		"order_products.csv"
	private val OUTPUT_DIR_REMOTE =
		"out/"

	private val DATASET_PATH =
		if (IS_LOCAL) DATASET_PATH_LOCAL
		else DATASET_PATH_REMOTE
	private val OUTPUT_DIR =
		if (IS_LOCAL) OUTPUT_DIR_LOCAL
		else OUTPUT_DIR_REMOTE


	def main(args: Array[String]): Unit = {

		FileUtils.forceMkdir(new File(OUTPUT_DIR))

		Util.executeWithTime("mapCartesianReduce", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce)

		// split
		//Util.executeWithTime("splitInside", DATASET_PATH, OUTPUT_DIR, SplitTests.splitInside)
		//Util.executeWithTime("splitBefore", DATASET_PATH, OUTPUT_DIR, SplitTests.splitBefore)

		Time.printAllTimes()

	}

}