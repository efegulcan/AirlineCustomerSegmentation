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

    println("Cleaned Data:")
    AirlineSegmentation.showDataFrame(cleanedData)

    val segmentedData = AirlineSegmentation.segmentPassengers(cleanedData)
    segmentedData.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/SegmentedData.csv")

    println("Segmented Data:")
    AirlineSegmentation.showDataFrame(segmentedData)

    val frequentTravelMonthsData = AirlineSegmentation.mostFrequentTravelMonths(cleanedData)
    frequentTravelMonthsData.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/FrequentTravelMonthsData.csv")

    println("Most Frequent Travel Months for Each Country:")
    AirlineSegmentation.showDataFrame(frequentTravelMonthsData)

    val ageSegmentedData = AirlineSegmentation.segmentPassengersByAge(cleanedData)
    ageSegmentedData.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/AgeSegmentedData.csv")
    println("Age Segmented Data:")
    AirlineSegmentation.showDataFrame(ageSegmentedData)

    val genderSegmentedData = AirlineSegmentation.segmentPassengersByGender(cleanedData)
    genderSegmentedData.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/GenderSegmentedData.csv")
    println("Gender Segmented Data:")
    AirlineSegmentation.showDataFrame(genderSegmentedData)

    val nationalitySegmentedData = AirlineSegmentation.segmentPassengersByNationality(cleanedData)
    nationalitySegmentedData.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/NationalitySegmentedData.csv")
    println("Nationality Segmented Data:")
    AirlineSegmentation.showDataFrame(nationalitySegmentedData)

    val flightStatusSegmentedData = AirlineSegmentation.segmentFlightsByStatus(cleanedData)
    flightStatusSegmentedData.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/FlightStatusSegmentedData.csv")
    println("Flight Status Segmented Data:")
    AirlineSegmentation.showDataFrame(flightStatusSegmentedData)

    spark.stop()
  }
}
