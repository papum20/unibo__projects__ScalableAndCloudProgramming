package util

import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{DataOutputStream, File, FileOutputStream, OutputStream}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Map
import scala.collection.AbstractMap

object Util {

	val DEBUG = false


	def executeWithTime[T](write: (SparkContext, T, String) => Unit) (
		tag: String,
		path_input: String, dir_output: String,
		block: (SparkContext, String) => T
	): Unit = {

		println("Launching: " + tag)

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val rdd = Time.printTime(tag, {
			block(sc, path_input)
		})

		Time.printTime( s"${tag}_write", {
			write(sc, rdd, dir_output)
		})
		sc.stop()

	}

	def executeWithTimeRDD[T](write: (SparkContext, RDD[T], String) => Unit) (
		tag: String,
		path_input: String, dir_output: String,
		block: (SparkContext, String) => RDD[T]
	): Unit = {

		println("Launching: " + tag)

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val rdd = Time.printTime(tag, {
			val res = block(sc, path_input)
				.persist()
println("done")
			//val c = res.count()
			// action, to measure time
			//println("Count: " + c)
			res
		})

		Time.printTime( s"${tag}_write", {
			write(sc, rdd, dir_output)
		})
		sc.stop()

	}

	def executeWithTimeRDD(
	   tag: String,
	   path_input: String, dir_output: String,
	   block: (SparkContext, String) => RDD[String]
   	): Unit =
		executeWithTimeRDD(writeOutput_noCoalesce)(tag, path_input, dir_output, block)

	def getPairs(elems: Iterable[Int]): Iterable[(Int, Int)] =
		for {
			a <- elems
			b <- elems
			// 1. avoid comparison when they're equal
			// 2. don't repeat pairs
			// 3. compute fewer pairs
			if a < b
		} yield (a,b)

	/**
	 *
	 * @param elems
	 * @return (pair, 1)
	 */
	def getPairs2(elems: Iterable[Int]): Iterable[((Int, Int), Int)] =
		for {
			a <- elems
			b <- elems
			// 1. avoid comparison when they're equal
			// 2. don't repeat pairs
			// 3. compute fewer pairs
			if a < b
		} yield ((a,b), 1)


	def printMem(): Unit = {

		val runtime = Runtime.getRuntime
		println(
			"max: "		+ runtime.maxMemory()	+ "\n" +
			"free: "	+ runtime.freeMemory()	+ "\n" +
			"total: "	+ runtime.totalMemory()	+ "\n"
		)
	}


	def writeOutput(sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		val tmp_path = out_path + "_tmp"

		Files.deleteIfExists(Paths.get(out_path))
		Files.deleteIfExists(Paths.get(tmp_path))
		rdd.coalesce(1).saveAsTextFile(tmp_path)

		// Get the single part file from the temporary folder and rename it
		val tmp_file = Files.list(Paths.get(tmp_path))
			.filter(path => path.getFileName.toString.startsWith("part-"))
			.findFirst()
			.get()

		Files.move(tmp_file, Paths.get(out_path), StandardCopyOption.REPLACE_EXISTING)
		Files.deleteIfExists(Paths.get(tmp_path))
	}

	def writeOutput_gStorage(storage: Storage, bucket_name: String)
							(sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		val tmp_path = out_path + "_tmp"

		GStorage.deleteFile(storage, bucket_name, tmp_path)
		GStorage.deleteFile(storage, bucket_name, out_path)

		rdd.coalesce(1).saveAsTextFile(tmp_path)

		// Get the single part file from the temporary folder and rename it
		val tmp_file = Files.list(Paths.get(tmp_path))
			.filter(path => path.getFileName.toString.startsWith("part-"))
			.findFirst()
			.get()

		Files.move(tmp_file, Paths.get(out_path), StandardCopyOption.REPLACE_EXISTING)
		GStorage.deleteFile(storage, bucket_name, tmp_path)
	}

	def writeOutput_noCoalesceNoRename(sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		Files.deleteIfExists(Paths.get(out_path))
		rdd.saveAsTextFile(out_path)
	}

	def writeOutput_noCoalesce(sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		val tmp_files = Files.list(Paths.get(out_path))
			.filter(path => path.getFileName.toString.startsWith("part-"))
			.iterator()
	}

	def writeOutput_noCoalesce_gStorage(storage: Storage, bucket_name: String)
									   (sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		val tmp_path = out_path + "_tmp/"
		val _out_path = out_path + "/"

		GStorage.deleteFile(storage, bucket_name, tmp_path)
		GStorage.deleteFile(storage, bucket_name, _out_path)
		rdd.saveAsTextFile(tmp_path)

		val tmp_files = Files.list(Paths.get(tmp_path))
			.filter(path => path.getFileName.toString.startsWith("part-"))
			.iterator()

		val out = Files.newOutputStream(Paths.get(_out_path))
		while (tmp_files.hasNext) {
			val tmp_file = tmp_files.next()
			Files.copy(tmp_file, out)
		}
		out.close()

		GStorage.deleteFile(storage, bucket_name, tmp_path)
	}

	def writeOutput_noCoalesce_noStrings(sc: SparkContext, rdd: RDD[((Int, Int), Int)], out_path: String): Unit = {

		val tmp_path = out_path + "_tmp"

		Files.deleteIfExists(Paths.get(out_path))
		Files.deleteIfExists(Paths.get(tmp_path))
		rdd.saveAsTextFile(tmp_path)

		val tmp_files = Files.list(Paths.get(tmp_path))
			.filter(path => path.getFileName.toString.startsWith("part-"))
			.iterator()

		val out = Files.newOutputStream(Paths.get(out_path))
		while (tmp_files.hasNext) {
			val tmp_file = tmp_files.next()
			Files.copy(tmp_file, out)
		}
		out.close()

		Files.deleteIfExists(Paths.get(tmp_path))
	}

	def writeOutput_noCoalesce_concurrentMap(sc: SparkContext, v: Map[(Int, Int), Int], out_path: String): Unit = {

		new File(out_path).delete()

		val out = new DataOutputStream( new FileOutputStream(new File(out_path)) )
		v.forEach { (k, v) =>
			out.writeChars(k._1 + "," + k._2 + "," + v + "\n")
		}
		out.close()
	}
	def writeOutput_noCoalesce_map(sc: SparkContext, v: AbstractMap[(Int, Int), Int], out_path: String): Unit = {

		new File(out_path).delete()

		val out = new DataOutputStream( new FileOutputStream(new File(out_path)) )
		v.foreach { e =>
			out.writeChars(e._1._1 + "," + e._1._2 + "," + e._2 + "\n")
		}
		out.close()
	}

}
