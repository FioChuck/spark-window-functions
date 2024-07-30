import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

trait WindowAvg {

  def wAvg(df: DataFrame, spark: SparkSession): DataFrame = {

    val windowSpec = Window
      .partitionBy(
        "title"
      )
      .orderBy(
        col("datehour").cast("timestamp").cast("long")
      )
      .rangeBetween(-23 * 60 * 60, 0)

    val resultDF =
      df.withColumn("rolling_avg_views", avg("views").over(windowSpec))

    resultDF

  }
}
