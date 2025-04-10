import functions.MapPairsReduce
import util.{Time, Util}

object Main {

	private val DATASET_PATH_LOCAL =
		"/home/papum/programm/unibo/projects/scalableAndCloudProgramming/src/main/resources/order_products.csv"
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

		var args_n = args.length
		// make sure -- is not read as argument
		if (args(0) == "--") {
			args_n -= 1
			for (i <- 0 until args_n)
				args(i) = args(i + 1)
		}

		if (args_n < 4) {
			usage()
			return
		}
		val version		= args(0).toInt
		val is_local	= args(1) == "true"
		val local_mode	= args(2) == "true"

		val out_path = args(3)

		if (!is_local && args.length < 5) {
			usage()
			return
		}
		val bucket_name = if (is_local) null else args(4)


		val DATASET_PATH =
			if (is_local) DATASET_PATH_LOCAL
			else s"gs://$bucket_name/$DATASET_PATH_REMOTE"
		val OUTPUT_DIR =
			if (is_local) out_path
			else s"gs://$bucket_name/$out_path"

		println("Path dataset: "	+ DATASET_PATH )
		println("Path output dir: "	+ OUTPUT_DIR )
		//Util.printMem()


		/* Run */

		//if (is_local)
		//	FileUtils.forceMkdir(new File(OUTPUT_DIR))

		version match {
			case 1 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce1",									local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce)
			case 2 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce2",									local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce2)
			case 3 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce3",									local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce3)
			case 4 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce4",									local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce4)
			case 5 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce5",									local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce5)
			case 6 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce_groupByKey",						local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce6)
			case 7 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce_reduceByKey",						local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce7)
			case 8 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce_noStrings)("mapPairsReduce_reduceByKey_noString", 	local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce8)
			case 9 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce_groupByKey_reduceByKey",			local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce9)
			case 10 =>	Util.executeWithTimeRDD(Util.writeOutput_noCoalesce)("mapPairsReduce_groupByKey_reduceByKey_match",		local_mode, DATASET_PATH, OUTPUT_DIR, MapPairsReduce.mapPairsReduce10)
		}

		/* OTHER TESTS */

		//Util.executeWithTime(Util.writeOutput_noCoalesce_concurrentMap)("mapPairsAggregateConcurrent", DATASET_PATH, OUTPUT_DIR, MapPairsAggregate.mapPairsAggregateConcurrent)
		//Util.executeWithTime(Util.writeOutput_noCoalesce_map)("mapPairsAggregate", DATASET_PATH, OUTPUT_DIR, MapPairsAggregate.mapPairsAggregate)

		Time.printAllTimes()

	}


	private def usage(): Unit = {
		println("Usage: VERSION IS_LOCAL={true|false} LOCAL_MODE={true|false} {REMOTE_OUT_PATH|LOCAL_OUT_PATH} [BUCKET_NAME]")
		println("    VERSION: algorithm version, 1 to 10")
		println("    IS_LOCAL: whether running in local")
		println("    LOCAL_MODE: whether running Spark in local mode (probably needed in local, or with a single worker)")
		println("    REMOTE_OUT_PATH|LOCAL_OUT_PATH: remote or local output path")
		println("    BUCKET_NAME: if not in local, needed to specify the Google Cloud bucket name")
	}

}