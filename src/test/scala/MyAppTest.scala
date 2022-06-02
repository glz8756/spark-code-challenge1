import com.ma.analytics.Model.metadataSchema
import com.ma.analytics.SparkUtil.readCSV
import org.scalatest.FlatSpec

class MyAppTest extends FlatSpec with MASharedSparkContext  {

  //def readCSV(p:String) = spark.read.option("header",true).option("quote","\"").option("escape","\"").csv(p)

  "a MyApp" should "test something" in {
    val basePath = "src/main/resources/"

    val df_movies_metadata  = readCSV(spark, metadataSchema, s"${basePath}/movies_metadata/movies_metadata.csv.gz", ",", true)
    df_movies_metadata.show(5, truncate = false)


    val df_ratings = spark.read
      .format("org.apache.spark.csv")
        .option("header", false)
      .schema(schema = metadataSchema)
      .csv(s"${basePath}/ratings/20171201/partition1/ratings.20171201.partition1.csv.gz")

    df_ratings.show(5, truncate = false)


  }
}
