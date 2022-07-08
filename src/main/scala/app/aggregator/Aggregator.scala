package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  //var title_name_avg = Map[String, Double]()
  @transient var mysc = sc
  var ratings_avg: RDD[(Int, (Double, Double, Boolean))] =  null
  var title_map: Map[Int, (String, Set[String])] = null
  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */
    /*
  def sum(a:Double, b:Double):Double ={
    a + b
  }*/
  def init(
          //ratings: userID, titleID, prev rating, new rating, timestamp.
          //title: title ID, title name, title’s keywords.
            ratings : RDD[(Int, Int, Option[Double], Double, Int)],
            title : RDD[(Int, String, List[String])]
          ) : Unit = {
      var ratings_pair = ratings.map(r => (r._2, r._4))
      var title_pair = title.map(t => (t._1, (t._2, t._3.toSet)))
      title_map =  title_pair.collect().toMap
      val join_result = title_pair.leftOuterJoin(ratings_pair)
      //flag to show if this title is rated or not
      var title_rating = join_result.map(x =>
        x._2._2 match {
          case None => (x._1, (0.0,false))
          case _ => (x._1, (x._2._2.get, true))
      }
      )

      var rating_cnt = title_rating.countByKey().map(x => (x._1, x._2*1.0))

      var rating_sum = title_rating.reduceByKey((x,y)=>(x._1 + y._1, x._2))

      //key = title_id
      //value = avg, cnt
      ratings_avg = rating_sum.map(x => {
        rating_cnt.get(x._1) match {
          case None => (x._1, (0.0, 0.0, x._2._2))//impossible go here
          case temp => x._2._2 match {
            case true => (x._1, (x._2._1 / temp.get, temp.get, x._2._2))
            case false => (x._1, (0.0, 0.0, x._2._2))
          }
        }
      }).cache()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult() : RDD[(String, Double)] = {
    //sc.parallelize(title_name_avg.toSeq)
    ratings_avg.map(x => (title_map.get(x._1).get._1, x._2._1))
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
    def contains(test:Set[String], target:List[String]): Boolean ={
      for(str <- target){
        if(!test.contains(str)){
          return false
        }
      }
      return true
    }
  def getKeywordQueryResult(keywords : List[String]) : Double = {
    var valid_ratings = ratings_avg
      .filter(x => x._2._3)
      .filter(x => contains(title_map.get(x._1).get._2, keywords))
    valid_ratings.isEmpty() match{
      case true => -1.0
      case false =>{
            def seqOp = (x: Double, y: (Int, (Double, Double, Boolean))) => x+ y._2._1
            def combOp = (x:Double, y:Double) => x + y
            val sum = valid_ratings.aggregate(0.0)(seqOp, combOp)
            sum/valid_ratings.count()
        }
      }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = {
        var delta_p = mysc.parallelize(delta_.toSeq)

        var update = delta_p.map(x => x._3 match {
          case None => (x._2, (x._4, 1.0))
          case _ => (x._2, (x._4 - x._3.get, 0.0))
        })
        .reduceByKey((x:(Double, Double),y:(Double, Double)) => (x._1 + y._1, x._2 + y._2))
        .collect().toMap
        ratings_avg.unpersist()
        ratings_avg = ratings_avg.map(x => update.get(x._1) match {
          case None => x
          case temp => (x._1, ((x._2._1 * x._2._2 + temp.get._1)/(x._2._2 + temp.get._2), (x._2._2 + temp.get._2), true ))
        }).cache()
  }
}
