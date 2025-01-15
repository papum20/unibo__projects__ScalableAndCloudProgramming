package util

import scala.collection.mutable

object Time {

	private val times = new mutable.LinkedHashMap[String, Long]()


	def getAllTimes(): Map[String, Long] =
		times.toMap


	/**
	 * Evaluate an expression and save the elapsed time with the given tag.
	 */
	def time[R](tag: String, block: => R): R = {
		val t0 = System.nanoTime()
		val result = block    // call-by-name
		val t1 = System.nanoTime()
		val elapsed = (t1 - t0)/1000000
		times.update(tag, elapsed)
		result
	}


	private def print(tag: String, time: Long): Unit = {
		println("Elapsed time (" + tag + "): " + time + "ms")
	}

	/**
	 * Evaluate an expression and print the elapsed time.
	 */
	def printTime[R](tag: String, block: => R): R = {
		val result = time(tag, block)
		print(times.last._1, times.last._2)
		result
	}

	def printAllTimes(): Unit = {
		for (entry <- times) {
			print(entry._1, entry._2)
		}
	}

}
