package org.abidi.jasser

/**
 * scala spark maven project
 *
 */

import org.apache.spark.sql.SparkSession
object App {
  def main(args : Array[String])={
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    import spark.implicits._
    (1 to 100).toDF().show
  }
  println( "Hello World!" )
}
