package util

import scala.collection.mutable

object Time {

	private val times = new mutable.LinkedHashMap[String, mutable.MutableList[Long]]()


	def getAllTimes(): Map[String, mutable.MutableList[Long]] =
		times.toMap


	/**
	 * Evaluate an expression and save the elapsed time with the given tag.
	 */
	def time[R](tag: String, block: => R): R = {
		val t0 = System.nanoTime()
		val result = block    // call-by-name
		val t1 = System.nanoTime()
		val elapsed = (t1 - t0)/1000000
		if (times.contains(tag)) {
			times.update(tag, times(tag) :+ elapsed)
		} else {
			times.put(tag, new mutable.MutableList[Long] :+ elapsed)
		}
		result
	}


	private def print(tag: String, time: Option[mutable.MutableList[Long]]): Unit = {
		//println("Elapsed time (" + tag + "), ms: " + time.mkString(",") )
		println(tag + " (ms): " + time.mkString(",") )
	}

	/**
	 * Evaluate an expression and print the elapsed time.
	 */
	def printTime[R](tag: String, block: => R): R = {
		val result = time(tag, block)
		print(tag, times.get(tag))
		result
	}

	def printAllTimes(): Unit = {
		for (key <- times.keys) {
			print(key, times.get(key))
		}
	}

}
