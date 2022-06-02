package com.ma.analytics


import com.ma.analytics.SparkUtil.readCSV
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MyApp {

  def main(args: Array[String]) = {


    val spark = SparkSession.builder().
      appName("spark-code-challenge")
      .master("local[*]")
      .getOrCreate()

    // data cleaning
    val moviesMetaDF = readCSV(spark, Model.metadataSchema, "src/main/resources/movies_metadata/movies_metadata.csv.gz", ",", true)
      .dropDuplicates().na.drop()
    val ratingDF = SparkUtil.readCSV(spark, Model.ratingsSchema, "src/main/resources/ratings/20171201/*/*", ",", false)


    //Bucketing use 1024 buckets under the assumption that the file size is ~2TB
    ratingDF.write
      .bucketBy(1024, "movie_id")
      .sortBy("movie_id")
      .mode("overwrite")
      .saveAsTable("bucketed_rating")

    val ratingBucketedDF = spark.table("bucketed_rating")

    val windowSpec = Window.partitionBy("user_id", "movie_id").orderBy(col("ts").desc_nulls_last)
    val latestMovieRatingByUser = ratingBucketedDF.withColumn("row_number", row_number.over(windowSpec)).where(col("row_number") === 1)

    val movieRatingJoinedDF = latestMovieRatingByUser.join(broadcast(moviesMetaDF), latestMovieRatingByUser.col("movie_id") === moviesMetaDF.col("id"))

    val avgMovieRating = movieRatingJoinedDF
      .groupBy(col("movie_id"))
      .agg(
        avg("rating"),
        count("*").as("number_of_votes")
      ).withColumn("avg(movie_rating)", format_number(col("avg(rating)"), 1)).drop("avg(rating)")

    val resultAvgMovieRating = moviesMetaDF.join(avgMovieRating, moviesMetaDF.col("id") === avgMovieRating.col("movie_id"))
      .select(col("movie_id"),
        col("title").as("movie_title"),
        col("runtime").as("movie_runtime"),
        col("avg(movie_rating)"),
        col("number_of_votes")
      )

    resultAvgMovieRating.show()


//    resultAvgMovieRating.write
//    .format("csv")
//    .option("header", "true")
//    .option("sep", ",")
//    .save("src/main/resources/data/avgMovieRating.csv")



  }


}
