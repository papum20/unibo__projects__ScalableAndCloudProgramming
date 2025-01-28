import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem
import functions.MapCartesianReduce
import org.apache.commons.io.FileUtils
import util.{GStorage, Time, Util}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors

object Main {

	private val DATASET_PATH_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/src/main/resources/order_products.csv"
	private val OUTPUT_DIR_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/out/"
	private val DATASET_PATH_REMOTE =
		"order_products.csv"


	/**
	 *
	 * @param args
	 * - `IS_LOCAL={true|false}` : whether running in local or remote
	 * - `BUCKET_NAME` : bucket name, required when in remote
	 */
	def main(args: Array[String]): Unit = {

		/* Args */

		if (args.length < 1) {
			usage()
			return
		}
		val is_local = args(0) == "true"

		if (!is_local && args.length < 3) {
			usage()
			return
		}
		val bucket_name = if (is_local) null else args(1)
		val remote_out_path = if (is_local) null else args(2)


		val DATASET_PATH =
			if (is_local) DATASET_PATH_LOCAL
			else s"gs://${bucket_name}/${DATASET_PATH_REMOTE}"
			//else DATASET_PATH_REMOTE
			//else fs.getPath(DATASET_PATH_REMOTE).toAbsolutePath.toString
		val OUTPUT_DIR =
			if (is_local) OUTPUT_DIR_LOCAL
			else s"gs://${bucket_name}/${remote_out_path}"
			//else fs.getPath(OUTPUT_DIR_REMOTE).toAbsolutePath.toString

		println("Path dataset: " + DATASET_PATH)
		println("Path output dir: " + OUTPUT_DIR)


		/* Run */

		if (is_local)
			FileUtils.forceMkdir(new File(OUTPUT_DIR))
Util.printMem()
		//Util.executeWithTime(Util.writeOutput)("mapCartesianReduce", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce)
		//Util.executeWithTime(Util.writeOutput)("mapCartesianReduce2", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce2)
		//Util.executeWithTime(Util.writeOutput)("mapCartesianReduce4", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce4)
		//Util.executeWithTime(Util.writeOutput_noCoalesceNoRename)("mapCartesianReduce3partitionedNoRename", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTime(Util.writeOutput_noCoalesce)("mapCartesianReduce3partitioned", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTime(Util.writeOutput_noCoalesceNoRename)("mapCartesianReduce3partitionedNoRename", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTime(Util.writeOutput_noCoalesce_concurrentMap)("mapCartesianAggregateConcurrent", DATASET_PATH, OUTPUT_DIR, MapCartesianAggregate.mapCartesianAggregateConcurrent)
		//Util.executeWithTime(Util.writeOutput_noCoalesce_map)("mapCartesianAggregate", DATASET_PATH, OUTPUT_DIR, MapCartesianAggregate.mapCartesianAggregate)
		Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapCartesianReduce3", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTimeRDD("mapCartesianReduce_groupByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce6)
		//Util.executeWithTimeRDD("mapCartesianReduce_reduceByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce7)
		//Util.executeWithTimeRDD(Util.writeOutput_noCoalesce_noStrings)("mapCartesianReduce_reduceByKey_noString", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce8)
		//Util.executeWithTimeRDD("mapCartesianReduce_groupByKey_reduceByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce9)
		//Util.executeWithTimeRDD(Util.writeOutput)("mapCartesianReduce3", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTimeRDD("mapCartesianReduce_groupByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce6)
		//Util.executeWithTimeRDD("mapCartesianReduce_reduceByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce7)
		//Util.executeWithTimeRDD(Util.writeOutput_noCoalesce_noStrings)("mapCartesianReduce_reduceByKey_noString", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce8)
		//Util.executeWithTimeRDD("mapCartesianReduce_groupByKey_reduceByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce9)
		//Util.executeWithTimeRDD(Util.writeOutput)("mapCartesianReduce3", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
		//Util.executeWithTimeRDD("mapCartesianReduce_groupByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce6)
		//Util.executeWithTimeRDD("mapCartesianReduce_reduceByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce7)
		//Util.executeWithTimeRDD(Util.writeOutput_noCoalesce_noStrings)("mapCartesianReduce_reduceByKey_noString", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce8)
		//Util.executeWithTimeRDD("mapCartesianReduce_groupByKey_reduceByKey", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce9)

		// split
		//Util.executeWithTime("splitInside", DATASET_PATH, OUTPUT_DIR, SplitTests.splitInside)
		//Util.executeWithTime("splitBefore", DATASET_PATH, OUTPUT_DIR, SplitTests.splitBefore)

		Time.printAllTimes()

		//if (!is_local && fs != null)
		//	fs.close()

	}


	def usage(): Unit = {
		println("Usage: IS_LOCAL={true|false} [BUCKET_NAME] [REMOTE_OUT_PATH]")
	}

}