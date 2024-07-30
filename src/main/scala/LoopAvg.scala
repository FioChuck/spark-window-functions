import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{min, max}
import com.google.common.collect.ImmutableMap

trait LoopAvg {

  def lAvg(df: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._

    val schema = StructType(
      Seq(
        StructField("company", StringType, nullable = true),
        StructField("max(datehour)", TimestampType, nullable = true),
        StructField("avg(views)", LongType, nullable = true)
      )
    )

    val emptyRDD = spark.sparkContext.emptyRDD[Row]

    var resultDF = spark.createDataFrame(emptyRDD, schema)

    val startDateTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0)
    val endDateTime = LocalDateTime.of(2024, 2, 1, 0, 0, 0)

    val companies = Array("Microsoft", "Amazon", "Google")

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val dateTimes = Iterator
      .iterate(startDateTime)(_.plusHours(1))
      .takeWhile(!_.isAfter(endDateTime))
      .map(formatter.format(_))
      .toArray

    companies.foreach { company =>
      var cnt = 0;

      dateTimes.foreach { dateTimeString =>
        var min = dateTimes(0)

        if (cnt <= 23) {
          min = dateTimes(0)
        } else {
          min = dateTimes(cnt - 23 - 1)
        }

        val max = dateTimes(cnt)

        val filteredDF = df
          .filter(
            $"datehour".between(min, max)
          )
          .filter($"datehour".between(min, max))
          .filter($"title" === company)
          .groupBy("title")
          .agg(ImmutableMap.of("datehour", "max", "views", "avg"))
          .withColumnRenamed("title", "company")

        resultDF = resultDF
          .union(filteredDF)

        cnt += 1

      }
    }

    val outputDF = resultDF
      .join(
        df,
        ($"company" === $"title") && ($"datehour" === $"max(datehour)")
      )
      .withColumnRenamed(
        "avg(views)",
        "rolling_avg_views"
      )
      .drop("max(datehour)")
      .drop("company")
      .orderBy("title", "datehour")
      .select("datehour", "title", "views", "rolling_avg_views")

    outputDF

  }

}
