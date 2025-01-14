import org.apache.spark.SparkContext, org.apache.spark.SparkConf, org.apache.commons.io.FileUtils, org.apache.spark.HashPartitioner, java.io._

object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("pageRank").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputFile = "/Users/zavattar/IdeaProjects/AA18-19/soc_Epinions.txt"
    val outputFile = "/Users/zavattar/IdeaProjects/AA18-19/pageRank"
    val input = sc.textFile(inputFile)

    val edges = input.map(s => (s.split("\t"))).
      map(a => (a(0).toInt,a(1).toInt))
    val links = edges.groupByKey().
      partitionBy(new HashPartitioner(4)).persist()
    var ranks = links.mapValues(v => 1.0)

    for(i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, rank)) =>
          uLinks.map(t => (t, rank / uLinks.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).
        mapValues(v => 0.15+0.85*v)
    }

    FileUtils.deleteDirectory(new File(outputFile))
    ranks.saveAsTextFile(outputFile)
    sc.stop()
  }
}