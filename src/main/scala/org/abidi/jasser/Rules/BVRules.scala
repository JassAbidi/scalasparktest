package org.abidi.jasser.Rules

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.count

object BVRules {

  def tagWithCountByKey(columnName:String, keys: Column*)(df: DataFrame):DataFrame ={
    val keysWindow = Window.partitionBy(keys:_*)
    df.withColumn(columnName,count(keys(0)).over(keysWindow))
  }


}
