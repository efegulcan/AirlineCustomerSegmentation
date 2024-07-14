import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AirlineSegmentation {

  def segmentPassengers(df: DataFrame): DataFrame = {
    val avgVisits = df.groupBy("Passenger ID")
      .count()
      .agg(avg("count"))
      .first()
      .getDouble(0)

    val visitsPerCustomer = df.groupBy("Passenger ID", "Gender", "Age")
      .count()
      .withColumnRenamed("count", "visits")

    visitsPerCustomer.select(
      col("Passenger ID"),
      col("Gender"),
      col("Age"),
      col("visits"),
      lit(avgVisits).alias("avgVisits"),
      when(col("visits") > avgVisits, lit("Frequent Visitor"))
        .otherwise(lit("Infrequent Visitor"))
        .alias("Segment")
    ).orderBy(desc("visits"))
  }

  def showDataFrame(df: DataFrame, numRows: Int = 20): Unit = {
    df.show(numRows)
  }
}
