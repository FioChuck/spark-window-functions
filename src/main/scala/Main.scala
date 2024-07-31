import scala.math.random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import com.google.cloud.spark.bigquery._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends WindowAvg with LoopAvg {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark Window")
      // .config("spark.sql.session.timeZone", "America/New_York")
      // .config("spark.master", "local[*]") // local dev
      // .config("spark.log.leve", "ERROR") // local dev
      // .config(
      //   "spark.hadoop.fs.AbstractFileSystem.gs.impl",
      //   "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      // )
      // .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
      // .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      // .config(
      //   "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
      //   "/Users/chasf/Desktop/cf-data-analytics-c0c7b23bcaf4.json"
      // )
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("gs://cf-spark-temp/checkpoint")

    val jobType = args(0).toLowerCase
    val destTable = args(1).toLowerCase
    val pgPW = args(2)
    val pgURL = args(3)

    // val jobType = "loop"
    // val destTable = "cf-data-analytics.spark_window.wiki_views_loop"

    import spark.implicits._

    val pages = Seq("Google", "Amazon", "Microsoft")

    //////////////////////////////////////////////////////////////////////////

    val df =
      spark.read
        .bigquery("bigquery-public-data.wikipedia.pageviews_2024")
        .filter(to_date($"datehour").between("2024-01-01", "2024-02-1"))
        .filter($"title".isin(pages: _*))
        .filter($"wiki" === "en")
        .select($"datehour", $"title", $"views")

    // val resultDF: DataFrame = jobType match {
    //   case "window" =>
    //     wAvg(df, spark)
    //   case "loop" =>
    //     lAvg(df, spark)
    //   case _ =>
    //     throw new IllegalArgumentException(s"Invalid job type: $jobType")
    // }

    // resultDF.write
    //   .format("bigquery")
    //   .option("writeMethod", "direct")
    //   .mode("overwrite")
    //   .save(
    //     destTable
    //   )

    //////////////////////////////////////////////////////////////////////////

    // val pgUser = "postgres"
    // val pgTable = "wikipedia_pageviews"

    // var df = spark.read
    //   .format("jdbc")
    //   .option("url", pgURL)
    //   .option("dbtable", "wikipedia_pageviews")
    //   .option("user", "postgres")
    //   .option("password", pgPW)
    //   .option("driver", "org.postgresql.Driver")
    //   .load()

    //////////////////////////////////////////////////////////////////////////

    // df = df.checkpoint()

    // val resultDF = wAvg(df, spark)

    // val dfOut = resultDF.filter($"views" > 100)

    df.write
      .format("jdbc")
      .option("url", pgURL)
      .option("dbtable", "wikipedia_pageviews_limit")
      .option("user", "postgres")
      .option("password", pgPW)
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()

    //////////////////////////////////////////////////////////////////////////

    // df.write
    //   .format("parquet")
    //   .mode("overwrite")
    //   .save("gs://analytics-data-lake/wiki-data")

    // df.count()
    // print("done")

  }
}
