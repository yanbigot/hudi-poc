package com.yb

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._



object Main {


  val spark = SparkSession.builder
    .master("local")
    .appName("spark session example")
    .config("spark.jars.packages","org.apache.hudi:hudi-spark-bundle:0.5.0-incubating")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]) {
    quickstartReference
//    test1
  }

  def quickstartReference= {

    val tableName = "hudi_cow_table"
    val basePath = "file:///tmp/hudi_cow_table"
    val dataGen = new DataGenerator

    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

    df.show(false)

    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Overwrite).
      save(basePath)

    val roViewDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*/*/*")
    roViewDF.createOrReplaceTempView("hudi_ro_table")
    spark.sql("select partitionpath, fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show(1000,false)
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table").show(1000,false)
  }
}
