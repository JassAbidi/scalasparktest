package org.abidi.jasser

import org.abidi.jasser.Rules.BVRules
import org.apache.spark.sql.DataFrame
import org.scalatest._


class RuleSpec extends FlatSpec  with GivenWhenThen with Matchers with SparkSqlSpec {
  private val records = Array(
    ("1", "Michael", "A", 14),
    ("2", "Anand", "P", 14),
    ("3", "Carol", "B", 37),
    ("4", "Joe", "C", 37),
    ("5", "Mary-Anne", "P", 14),
    ("6", "George", "P", 77),
    ("7", "John", "C", 37),
    ("8", "David", "P", 77),
    ("9", "Zacary", "C", 37),
    ("10", "Eric", "B", 59),
    ("11", "Elizabeth", "P", 14),
    ("12", "Kumar", "A", 14)
  )
  private val recordsWithCountByKeys = Array(
    ("1", "Michael", "A", 14, 2),
    ("2", "Anand", "P", 14, 3),
    ("3", "Carol", "B", 37, 1),
    ("4", "Joe", "C", 37, 3),
    ("5", "Mary-Anne", "P", 14, 3),
    ("6", "George", "P", 77, 2),
    ("7", "John", "C", 37, 3),
    ("8", "David", "P", 77, 2),
    ("9", "Zacary", "C", 37, 3),
    ("10", "Eric", "B", 59, 1),
    ("11", "Elizabeth", "P", 14, 3),
    ("12", "Kumar", "A", 14, 2)
  )

  private var recordsDf: DataFrame = _
  private var recordsWithCountByKeysDf : DataFrame = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    val _sqlc = sqlc
    import _sqlc.implicits._

    val recordsDf = sc.parallelize(records).toDF("id","name","staemp","age")
    val recordsWithCountByKeysDf = sc.parallelize(recordsWithCountByKeys).toDF("id","name","staemp","age","count")
  }

  "The last name of all employees" should "be selected" in {
    val _sqlc = sqlc

    import _sqlc.implicits._

    val recordsDf = sc.parallelize(records).toDF("id","name","staemp","age")
    val recordsWithCountByKeysDf = sc.parallelize(recordsWithCountByKeys).toDF("id","name","staemp","age","count")
    BVRules.tagWithCountByKey("count",recordsDf.col("staemp"),recordsDf.col("age"))(recordsDf).collect() should contain theSameElementsAs (recordsWithCountByKeysDf.collect())
  }
}
