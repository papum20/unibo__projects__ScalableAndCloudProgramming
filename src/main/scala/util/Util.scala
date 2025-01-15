package util

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.io.{DataOutputStream, File, FileOutputStream, PrintWriter}
import scala.io.Source

object Util {


	def executeWithTime(
		tag: String,
		path_input: String, dir_output: String,
		block: (SparkContext, String) => RDD[String]
	): Unit = {

		println("Launching: " + tag)

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val rdd = Time.time[RDD[String]](tag, {
			block(sc, path_input)
		})

		writeOutput(rdd, dir_output + "/" + tag + ".csv")
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


	private def writeOutput(rdd: RDD[String], path_output: String): Unit = {

		new File(path_output).delete()
		val out = new PrintWriter(path_output)

		rdd.collect()
			.foreach { line =>
				out.write(line + "\n")
			}

		out.close()
	}

}
