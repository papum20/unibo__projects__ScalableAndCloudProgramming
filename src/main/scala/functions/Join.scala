package functions

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Join {

	private def productsCorrelations(path_input: String, path_output: String): Unit = {

		val conf = new SparkConf().setAppName("orderProducts").setMaster("local[*]")
		val sc = new SparkContext(conf)

		val in = sc.textFile(path_input)

		// should I first map?
		// is it better if elements are just split[0] instead of whole lines?

		val receipts_to_products = in.groupBy { line =>
			line.split(",")(0)
		}

		println( "Map:" )
		println( receipts_to_products.count() )
		println( receipts_to_products.take(5).mkString("\n") )

		val joined = receipts_to_products.join(receipts_to_products)

		println( "Joined:" )
		println( joined.count() )
		println( joined.take(5).mkString("\n") )

		FileUtils.deleteDirectory(new File(path_output))
		//ranks.saveAsTextFile(outputFile)
		sc.stop()
	}

}
