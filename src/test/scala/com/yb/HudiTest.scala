package com.yb

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.yb.utils.DataGenerator
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql.SaveMode.Append
import org.scalatest.{FlatSpec, Matchers}

class HudiTest extends FlatSpec with DataFrameSuiteBase with Matchers {

  import spark.implicits._

  def setUpDatabase = {
    spark.sql(
      """
        |SET hive.input.format=org.apache.hudi.hadoop.HoodieParquetInputFormat;
        |
      """.stripMargin)
    spark.sql("CREATE DATABASE IF NOT EXISTS bv LOCATION '/tmp/bv.db' ")
  }

  "Hive" should "initialize properly" in {
    this.setUpDatabase
    val data = DataGenerator.generateRandomData()
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data))
    df.write.partitionBy("elr").saveAsTable("bv.business_view")
    spark.sql("SELECT * FROM bv.business_view").show(truncate = false)
  }

  "Hudi" should "create Hive table" in {
    this.setUpDatabase
    val data = DataGenerator.generateRandomData(2)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data))

    val tableName = "hudi_business_view"
    val basePath = "file:///tmp/hudi_business_view"

    val bvHudiOptions = Map[String, String](
      HoodieWriteConfig.TABLE_NAME → "hudi_business_view",
      DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "ggi",
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "elr",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "sysDate",
      DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY → "true",
      DataSourceWriteOptions.HIVE_TABLE_OPT_KEY → tableName,
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY → "elr",
      DataSourceWriteOptions.HIVE_URL_OPT_KEY → "thrift://localhost:9083"
      //      ,DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY → classOf[MultiPartKeysValueExtractor].getName
    )
    df.write.
      format("org.apache.hudi").
      options(bvHudiOptions).
      mode(Append).
      save(basePath)


  }

  "Hudi" should "upsert data and then read all data and then only the last upserted ones" in {

    this.setUpDatabase
    val data = DataGenerator.generateRandomData(220000)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data))
    df.show
    println("generated entity objects: " + df.count)

    val tableName = "hudi_entity_table"
    val basePath = "file:///tmp/hudi_entity_table"

    df.write.format("org.apache.hudi").
      option(RECORDKEY_FIELD_OPT_KEY, "ggi").
      option(PRECOMBINE_FIELD_OPT_KEY, "sysDate").
      option(PARTITIONPATH_FIELD_OPT_KEY, "elr").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*").
      createOrReplaceTempView("hudi_ro_table")

    spark.sql("select ggi, elr, hh, col1, sysDate, executionId from  hudi_ro_table where elr = 'FRANCE'").show(100)
    spark.sql("select count(ggi) from  hudi_ro_table").show(10000)

    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime desc")
      .map(_.getString(0)).take(50)
    commits.foreach(println)

    val beginTime = commits(commits.length - 1) // commit time we are interested in

    // incrementally query data
    val incViewDF = spark.
      read.
      format("org.apache.hudi").
      option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL).
      option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "0").
      load(basePath)
    incViewDF.registerTempTable("hudi_incr_table")
    spark.sql("select `_hoodie_commit_time`, ggi, elr, hh, col1, sysDate, executionId from  hudi_incr_table").show()
    spark.sql("select count(ggi) from  hudi_incr_table").show(10000)
  }

  "Workforce administration bv" should "be created properly" in {
    val data = DataGenerator.generateBusinessViewData(100000)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data))

    val tableName = "workforce_administration"
    val basePath = "file:///tmp/workforce_administration"

    df.write.format("org.apache.hudi").
      option(RECORDKEY_FIELD_OPT_KEY, "mergeKey").
      option(PRECOMBINE_FIELD_OPT_KEY, "executionId").
      option(PARTITIONPATH_FIELD_OPT_KEY, "entityName").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    val fromHudiTableDf = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*")
    fromHudiTableDf.createOrReplaceTempView("hudi_ro_workforce_administration_table")

    val dfCount = df.count
    val fromHudiTableDfCount = fromHudiTableDf.count
    println("generated entity objects: " + dfCount)
    println("generated entity objects: " + fromHudiTableDfCount)

  }

  "Workforce administration bv" should "upsert properly for small amount of data" in {
    val data = DataGenerator.generateBusinessViewData(100)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data))

    val tableName = "workforce_administration"
    val basePath = "file:///tmp/workforce_administration"


    val startDate = System.nanoTime()

    df.write.format("org.apache.hudi").
      option(RECORDKEY_FIELD_OPT_KEY, "mergeKey").
      option(PRECOMBINE_FIELD_OPT_KEY, "executionId").
      option(PARTITIONPATH_FIELD_OPT_KEY, "entityName").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    val fromHudiTableDf = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*")
    fromHudiTableDf.createOrReplaceTempView("hudi_ro_workforce_administration_table")

    val dfCount = df.count
    val fromHudiTableDfCount = fromHudiTableDf.count
    println("generated entity objects: " + dfCount)
    println("generated entity objects: " + fromHudiTableDfCount)

    val endDate = System.nanoTime()
  }

  "Hudi" should "be understandable to yan" in {
    val data = DataGenerator.generateSimpleData(1)
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(data))
    val tableName = "hudi_simple_table"
    val basePath = "file:///tmp/hudi_simple_table"

    df.write.format("org.apache.hudi").
      option(RECORDKEY_FIELD_OPT_KEY, "key").
      option(PRECOMBINE_FIELD_OPT_KEY, "key").
      option(PARTITIONPATH_FIELD_OPT_KEY, "").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)




  }
}
