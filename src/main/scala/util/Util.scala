package util

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Util {

	val DEBUG = false


	def executeWithTime(
		tag: String,
		path_input: String, dir_output: String,
		block: (SparkContext, String) => RDD[String]
	): Unit = {

		println("Launching: " + tag)

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val rdd = Time.time(tag, {
			block(sc, path_input)
		})

		Time.time( s"${tag}_write", {
			writeOutput(sc, rdd, dir_output + "/" + tag + ".csv")
		})
		sc.stop()

	}

	def getPairs(elems: Iterable[Int]): Iterable[(Int, Int)] =
		for {
			a <- elems
			b <- elems
			// 1. avoid comparison when they're equal
			// 2. don't repeat pairs
			// 3. compute fewer pairs
			if a < b
		} yield (a,b)


	def printMem(): Unit = {

		val runtime = Runtime.getRuntime
		println(
			"max: "		+ runtime.maxMemory()	+ "\n" +
			"free: "	+ runtime.freeMemory()	+ "\n" +
			"total: "	+ runtime.totalMemory()	+ "\n"
		)
	}


	private def writeOutput(sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		val tmp_path = out_path + "_tmp"
		val fs = FileSystem.get(sc.hadoopConfiguration)

		//FileUtils.deleteDirectory(new File(out_path))
		fs.delete(new Path(out_path), true)
		rdd.coalesce(1).saveAsTextFile(tmp_path)

		// Get the single part file from the temporary folder and rename it
		val tmp_file = fs.globStatus(new Path(s"$tmp_path/part-*"))(0).getPath
		fs.rename(tmp_file, new Path(out_path))

		fs.delete(new Path(tmp_path), true)

	}

}
