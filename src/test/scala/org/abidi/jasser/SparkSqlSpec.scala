package org.abidi.jasser

import org.apache.spark.sql.hive.HiveContext
import org.scalatest.Suite

trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlc: HiveContext = _

  def sqlc: HiveContext = _sqlc

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = new HiveContext(sc)
  }

  override def afterAll(): Unit = {
    _sqlc = null

    super.afterAll()
  }

}
