package util

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{DataOutputStream, File, FileOutputStream, OutputStream}
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
			write(sc, rdd, dir_output + "/" + tag + ".csv")
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
			val c = res.count()
			// action, to measure time
			println("Count: " + c)
			res
		})

		Time.printTime( s"${tag}_write", {
			write(sc, rdd, dir_output + "/" + tag + ".csv")
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
		val fs = FileSystem.get(sc.hadoopConfiguration)

		//FileUtils.deleteDirectory(new File(out_path))
		fs.delete(new Path(out_path), true)
		fs.delete(new Path(tmp_path), true)
		rdd.coalesce(1).saveAsTextFile(tmp_path)

		// Get the single part file from the temporary folder and rename it
		val tmp_file = fs.globStatus(new Path(s"$tmp_path/part-*"))(0).getPath
		fs.rename(tmp_file, new Path(out_path))

		fs.delete(new Path(tmp_path), true)
	}

	def writeOutput_noCoalesceNoRename(sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		val fs = FileSystem.get(sc.hadoopConfiguration)

		fs.delete(new Path(out_path), true)
		rdd.saveAsTextFile(out_path)
	}

	def writeOutput_noCoalesce(sc: SparkContext, rdd: RDD[String], out_path: String): Unit = {

		val tmp_path = out_path + "_tmp"
		val fs = FileSystem.get(sc.hadoopConfiguration)

		fs.delete(new Path(out_path), true)
		fs.delete(new Path(tmp_path), true)
		rdd.saveAsTextFile(tmp_path)

		val out: OutputStream = new FileOutputStream(new File(out_path))
		for ( tmp_file <- fs.globStatus(new Path(s"$tmp_path/part-*")) ) {
			FileUtils.copyFile(new File(tmp_file.getPath.toUri), out)
		}
		out.close()

		fs.delete(new Path(tmp_path), true)
	}

	def writeOutput_noCoalesce_noStrings(sc: SparkContext, rdd: RDD[((Int, Int), Int)], out_path: String): Unit = {

		val tmp_path = out_path + "_tmp"
		val fs = FileSystem.get(sc.hadoopConfiguration)

		fs.delete(new Path(out_path), true)
		fs.delete(new Path(tmp_path), true)
		rdd.saveAsTextFile(tmp_path)

		val out: OutputStream = new FileOutputStream(new File(out_path))
		for ( tmp_file <- fs.globStatus(new Path(s"$tmp_path/part-*")) ) {
			FileUtils.copyFile(new File(tmp_file.getPath.toUri), out)
		}
		out.close()

		fs.delete(new Path(tmp_path), true)
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
