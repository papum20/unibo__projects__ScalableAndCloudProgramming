import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main2 {
	def main(args: Array[String]): Unit = {
		// Input and output paths are passed as command-line arguments
		val inputPath = args(0)
		val outputPath = args(1)

		// Initialize a SparkSession
		val spark = SparkSession.builder()
			.appName("CoPurchaseAnalysis")
			.getOrCreate()

		// Read the input CSV file and return an RDD of (Int, Int) tuples
		val rawRDD: RDD[(Int, Int)] = readCSV(spark, inputPath)

		// Process the RDD to calculate co-purchase counts
		val coPurchaseCounts = processCoPurchases(rawRDD)

		// Save the co-purchase counts to the specified output path
		coPurchaseCounts
			.map { case ((product1, product2), count) => s"$product1,$product2,$count" }
			.saveAsTextFile(outputPath)

		// Stop the SparkSession
		spark.stop()
	}

	/**
	 * Reads a CSV file and returns an RDD of (Int, Int) tuples.
	 * Each tuple represents a pair of product IDs from a single order.
	 *
	 * @param spark SparkSession
	 * @param filePath Path to the input CSV file
	 * @return RDD of (Int, Int) tuples
	 */
	private def readCSV(spark: SparkSession, filePath: String): RDD[(Int, Int)] = {
		// Read the file as an RDD of lines
		val lines = spark.sparkContext.textFile(filePath)
		// Split each line by comma and convert to (Int, Int) tuples
		lines.map { line =>
			val parts = line.split(",")
			(parts(0).trim.toInt, parts(1).trim.toInt)
		}
	}

	/**
	 * Processes the input RDD to calculate co-purchase counts.
	 * Groups products by order and then counts the number of times each pair of products is purchased together.
	 *
	 * @param data RDD of (Int, Int) tuples representing product pairs
	 * @return RDD of ((Int, Int), Int) tuples representing product pairs and their co-purchase counts
	 */
	private def processCoPurchases(data: RDD[(Int, Int)]): RDD[((Int, Int), Int)] = {
		// Group products by order ID
		val groupedProducts = data.groupByKey()
		// Generate all unique pairs of products for each order and count them
		val productPairs = groupedProducts.flatMap {
			case (_, products) =>
				val productList = products.toSet
				for {
					x <- productList
					y <- productList if x < y
				} yield ((x, y), 1)
		}
		// Reduce by key to count the occurrences of each product pair
		productPairs.reduceByKey(_ + _)
	}
}