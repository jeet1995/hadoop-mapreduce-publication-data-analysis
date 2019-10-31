package main.scala.com.publication.statistics.computation.bucketingByNumOfNodes

object KeyGenerator {


  /**
    * @param number Denotes the number which is to be asigned a bucket.
    * @param bucketSize Denoted the bucket size to which the number is assigned.
    *
    * @return The bucket itself.
    * */
  def generateKeyByBucket(number: Int, bucketSize: Int): String = {

    if (number == 0)
      number.toString
    else if (number % bucketSize > 0) {
      val mod = number % bucketSize
      (number - mod + 1).toString + "-" + (number + (bucketSize - mod)).toString
    } else {
      (number - bucketSize + 1).toString + "-" + number.toString
    }
  }
}
