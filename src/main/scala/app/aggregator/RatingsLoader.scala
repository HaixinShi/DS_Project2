package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val data = sc.textFile("src/main/resources"+path)//D:\EPFL\db\Project2\src\main\resources
      .map(f=> f.split("\\|"))
      .map(x => (x(0).toInt, x(1).toInt, None.asInstanceOf[Option[Double]], x(2).toDouble,x(3).toInt)).cache()
    data
  }
}
