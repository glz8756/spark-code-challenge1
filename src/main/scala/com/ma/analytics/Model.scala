package com.ma.analytics

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Model {

  val metadataSchema = StructType(Array(
    StructField("id", StringType),
    StructField("runtime", IntegerType),
    StructField("title", StringType)
  ))

  val ratingsSchema = StructType(Array(
    StructField("user_id", StringType, nullable = true),
    StructField("movie_id", StringType, nullable = true),
    StructField("rating", DoubleType, nullable = true),
    StructField("ts", LongType, nullable = true)
  ))


}
