import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem
import functions.MapCartesianReduce
import org.apache.commons.io.FileUtils
import util.{Time, Util}

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
	private val OUTPUT_DIR_REMOTE =
		""
		//"out/"

	private val GCLOUD_PROJECT_ID = "scalableandcloudprogramming24"


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

		if (!is_local && args.length < 2) {
			usage()
			return
		}
		val bucket_name = if (is_local) null else args(1)

		//val fs =
		//	if (!is_local) CloudStorageFileSystem.forBucket(bucket_name)
		//	else null


		//val folder = fs.getPath(".")
		//val stream = Files.list(folder)
		//stream
		//	.forEach( it => println(it.toAbsolutePath.toString) )
		//stream.close()

		//println("Folder " + folder)
		//val listOfFiles = folder.
		//if (listOfFiles != null) for (i <- 0 until listOfFiles.length) {
		//	if (listOfFiles(i).isFile) System.out.println("File " + listOfFiles(i).getAbsolutePath)
		//	else if (listOfFiles(i).isDirectory) System.out.println("Directory " + listOfFiles(i).getAbsolutePath)
		//}


		val DATASET_PATH =
			if (is_local) DATASET_PATH_LOCAL
			else s"gs://${bucket_name}/${DATASET_PATH_REMOTE}"
			//else fs.getPath(DATASET_PATH_REMOTE).toAbsolutePath.toString
		val OUTPUT_DIR =
			if (is_local) OUTPUT_DIR_LOCAL
			else s"gs://${bucket_name}/${OUTPUT_DIR_REMOTE}"
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
		Util.executeWithTimeRDD(Util.writeOutput)("mapCartesianReduce3", DATASET_PATH, OUTPUT_DIR, MapCartesianReduce.mapCartesianReduce3)
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
		println("Usage: IS_LOCAL={true|false} [BUCKET_NAME]")
	}

	/*
	main() {

		//val path = Paths.get(URI.create(OUTPUT_DIR + "/" + "out.csv"))
		//val path = Path.of("out.csv")
		//println("Path " + path.toAbsolutePath)
		//println("Path " + path.toFile.getAbsolutePath)

		//val out = new DataOutputStream( new FileOutputStream(path.toFile) )
		//out.writeChars("a,b,c\n")
		//out.close()


		//// Create a new GCS client
		//val storage = StorageOptions.newBuilder.setProjectId(projectId).build.getService
		//// The blob ID identifies the newly created blob, which consists of a bucket name and an object
		//// name
		//val blobId = BlobId.of(bucketName, objectName)
		//val blobInfo = BlobInfo.newBuilder(blobId).build
		//// The filepath on our local machine that we want to upload
		//val filePath = objectName
		//// upload the file and print the status
		//storage.createFrom(blobInfo, Paths.get(filePath))
		//System.out.println("File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName)
		}
	 */



	// upload file to GCS  // upload file to GCS
	//@throws[IOException]
	//def uploadFile(): Unit = {
	//	// Create a new GCS client
	//	val storage = StorageOptions.newBuilder.setProjectId(projectId).build.getService
	//	// The blob ID identifies the newly created blob, which consists of a bucket name and an object
	//	// name
	//	val blobId = BlobId.of(bucketName, objectName)
	//	val blobInfo = BlobInfo.newBuilder(blobId).build
	//	// The filepath on our local machine that we want to upload
	//	val filePath = "/tmp/sample.txt"
	//	// upload the file and print the status
	//	storage.createFrom(blobInfo, Paths.get(filePath))
	//	System.out.println("File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName)
	//}
	//
	//@throws[IOException]
	//def deleteFile(): Unit = {
	//	// Create a new GCS client and get the blob object from the blob ID
	//	val storage = StorageOptions.newBuilder.setProjectId(projectId).build.getService
	//	val blobId = BlobId.of(bucketName, objectName)
	//	val blob = storage.get(blobId)
	//	// delete the file and print the status
	//	blob.delete
	//	System.out.println("File " + objectName + " deleted from bucket " + bucketName)
	//}
	// download file from GCS  // download file from GCS
	//@throws[IOException]
	//def downloadFile(): Unit = {
	//	// we'll download the same file to another file path
	//	val filePath = "/tmp/sample_downloaded.txt"
	//	// Create a new GCS client and get the blob object from the blob ID
	//	val storage = StorageOptions.newBuilder.setProjectId(projectId).build.getService
	//	val blobId = BlobId.of(bucketName, objectName)
	//	val blob = storage.get(blobId)
	//	// download the file and print the status
	//	blob.downloadTo(Paths.get(filePath))
	//	System.out.println("File " + objectName + " downloaded to " + filePath)
	//}

}