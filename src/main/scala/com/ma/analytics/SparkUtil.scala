package com.ma.analytics

import org.apache.spark.sql.functions._
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

  def calcAvgMovieRating(movieRatingByUser: DataFrame, moviesMeta: DataFrame): DataFrame = {
    val avgMovieRating = movieRatingByUser
      .groupBy(col("movie_id"))
      .agg(
        avg("rating"),
        count("*").as("number_of_votes")
      ).withColumn("avg(movie_rating)", format_number(col("avg(rating)"), 1)).drop("avg(rating)")

    avgMovieRating.join(broadcast(moviesMeta), movieRatingByUser.col("movie_id") === moviesMeta.col("id"))
      .select(col("movie_id"),
        col("title").as("movie_title"),
        col("runtime").as("movie_runtime"),
        col("avg(movie_rating)"),
        col("number_of_votes")
      )
  }
}
