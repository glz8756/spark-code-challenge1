package com.ma.analytics

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtil {

  def readCSV(spark: SparkSession, schema: StructType, path: String, sep: String, header: Boolean): DataFrame = {
    spark.read
      .schema(schema)
      .option("header", header)
      .option("sep", sep)
      .csv(path)
  }
}
