import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Airline Customer Segmentation")
      .master("local[*]")
      .getOrCreate()

    val filePath = "data/AirlineDataset.csv"
    val reader = AirlineReader(spark, filePath)

    val cleanedData = reader.getCleanedDataFrame

    AirlineSegmentation.showDataFrame(cleanedData)

    val segmentedData = AirlineSegmentation.segmentPassengers(cleanedData)

    AirlineSegmentation.showDataFrame(segmentedData)

    spark.stop()
  }
}
