import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class AirlineReader(spark: SparkSession, filePath: String) {

  private val df: DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .na.fill("")
      .na.drop()
  }
  def getCleanedDataFrame: DataFrame = df

  def segmentPassengers: DataFrame = {
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

object AirlineReader {
  def apply(spark: SparkSession, filePath: String): AirlineReader = new AirlineReader(spark, filePath)
}

// Usage Example
object AirlineSegmentationApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Airline Customer Segmentation")
      .master("local[*]")
      .getOrCreate()

    val filePath = "data/AirlineDataset.csv"
    val reader = AirlineReader(spark, filePath)

    // Show cleaned data
    reader.showDataFrame(reader.getCleanedDataFrame)

    // Show segmented passengers
    reader.showDataFrame(reader.segmentPassengers)

    spark.stop()
  }
}
