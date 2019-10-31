package test.scala.com.publication.statistics.computation.bucketingByNumOfNodes

import main.scala.com.publication.statistics.computation.bucketingByNumOfNodes.KeyGenerator
import org.scalatest.FunSuite

class KeyGeneratorTest extends FunSuite {

  test("Key generator should return the right bucket.") {
    val bucketStr = KeyGenerator.generateKeyByBucket(3, 5)
    assert(bucketStr === "1-5")
  }

}
