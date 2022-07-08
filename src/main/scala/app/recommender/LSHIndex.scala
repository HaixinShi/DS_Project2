package app.recommender

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {
  private val minhash = new MinHash(seed)

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }
  var buckets :  Map[IndexedSeq[Int], List[(Int, String, List[String])]] = null
  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets()
    : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
      var ret = data.map(x => (minhash.hash(x._3), x)).groupByKey().map(x => (x._1, x._2.toList))
      buckets = ret.collect().toMap
      ret
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    if (buckets == null){
      getBuckets()
    }
    queries.map(x=> buckets.get(x._1) match {
      case None => (x._1, x._2, List.empty)
      case item => (x._1, x._2, item.get)
    })
  }
}
