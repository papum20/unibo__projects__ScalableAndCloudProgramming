import functions.{MapCartesianAggregate, MapCartesianReduce}
import org.apache.commons.io.FileUtils
import util.{Time, Util}

import java.io.File

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

		//Util.executeWithTime(Util.writeOutput)("mapCartesianReduce", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce)
		//Util.executeWithTime(Util.writeOutput)("mapCartesianReduce2", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce2)
		//Util.executeWithTime(Util.writeOutput)("mapCartesianReduce3", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTime(Util.writeOutput)("mapCartesianReduce4", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce4)
		//Util.executeWithTime(Util.writeOutput_noCoalesceNoRename)("mapCartesianReduce3partitionedNoRename", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTime(Util.writeOutput_noCoalesce)("mapCartesianReduce3partitioned", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTime(Util.writeOutput_noCoalesceNoRename)("mapCartesianReduce3partitionedNoRename", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTime(Util.writeOutput_noCoalesce_concurrentMap)("mapCartesianAggregateConcurrent", DATASET_PATH, OUTPUT_DIR, MapCartesianAggregate.mapCartesianAggregateConcurrent)
		Util.executeWithTime(Util.writeOutput_noCoalesce_map)("mapCartesianAggregate", DATASET_PATH, OUTPUT_DIR, MapCartesianAggregate.mapCartesianAggregate)

		// split
		//Util.executeWithTime("splitInside", DATASET_PATH, OUTPUT_DIR, SplitTests.splitInside)
		//Util.executeWithTime("splitBefore", DATASET_PATH, OUTPUT_DIR, SplitTests.splitBefore)

		Time.printAllTimes()

	}

}