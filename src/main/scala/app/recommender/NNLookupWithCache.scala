package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex : LSHIndex) extends Serializable {
  var cache:Map[IndexedSeq[Int], List[(Int, String, List[String])]] = null
  var histogram:  RDD[(IndexedSeq[Int], List[String])] = null
  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {
      var b = sc.parallelize(sc.broadcast(histogram.collect()).value)
      cache = lshIndex.lookup(b).map(x => (x._1,x._3)).collect().toMap
      histogram = null
  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]) = {
    cache = ext.value
  }

  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {
    var all_queries = lshIndex.hash(queries)
    var cnt = all_queries.countByKey().filter(x => x._2*1.0 > all_queries.count()*0.01)//cnt contains all quieries that are <= 1%
    histogram = all_queries.filter(x => cnt.get(x._1) != None)//all quieries that are <= 1% will be removed
    cache == null match {
      case true => (null,all_queries)
      case false =>{
        var miss = all_queries.filter(x => cache.get(x._1)==None)
        var hit = all_queries.filter(x => cache.get(x._1)!=None).map(x => (x._2,cache.get(x._1).get))
        (hit,miss)
      }
    }

  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    var split = cacheLookup(queries)
    var hit = split._1
    var miss = split._2
    var lookup = lshIndex.lookup(miss).map(x => (x._2, x._3))
    (hit == null, lookup == null) match{
      case (true, false) => lookup
      case (false, true) => hit
      case (false, false) => hit.union(lookup)
      case (true, true) => hit
    }
  }
}
