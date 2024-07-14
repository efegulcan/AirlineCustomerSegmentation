import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

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
      when(col("visits") > avgVisits, lit("Frequent Visitor"))
        .otherwise(lit("Infrequent Visitor"))
        .alias("Segment")
    ).orderBy(desc("visits")).drop("avgVisits")
  }

  def mostFrequentTravelMonths(df: DataFrame): DataFrame = {
    val dfWithDate = df.withColumn(
      "Departure Date",
      coalesce(
        to_date(col("Departure Date"), "M/d/yyyy"),
        to_date(col("Departure Date"), "MM-dd-yyyy")
      )
    )

    dfWithDate.filter(col("Departure Date").isNull)
      .select("Departure Date")
      .distinct()
      .show(false)

    val dfWithMonth = dfWithDate
      .withColumn("Month", date_format(col("Departure Date"), "MMMM"))
      .withColumn("Year", year(col("Departure Date")))

    val monthlyCounts = dfWithMonth.groupBy("Country Name", "Month")
      .count()
      .withColumnRenamed("count", "Frequency")

    val rankedMonths = monthlyCounts
      .withColumn("Rank", rank().over(Window.partitionBy("Country Name").orderBy(desc("Frequency"))))

    rankedMonths
      .filter(col("Rank") === 1)
      .select("Country Name", "Month", "Frequency")
      .orderBy("Country Name")
  }

  def segmentPassengersByAge(df: DataFrame): DataFrame = {
    df.withColumn("Age Group",
        when(col("Age") < 18, "Under 18")
          .when(col("Age").between(18, 25), "18-25")
          .when(col("Age").between(26, 35), "26-35")
          .when(col("Age").between(36, 45), "36-45")
          .when(col("Age").between(46, 60), "46-60")
          .otherwise("60+"))
      .groupBy("Age Group")
      .count()
      .orderBy(desc("count"))
  }

  def segmentPassengersByGender(df: DataFrame): DataFrame = {
    df.groupBy("Gender")
      .count()
      .orderBy(desc("count"))
  }

  def segmentPassengersByNationality(df: DataFrame): DataFrame = {
    df.groupBy("Nationality")
      .count()
      .orderBy(desc("count"))
  }

  def segmentFlightsByStatus(df: DataFrame): DataFrame = {
    df.groupBy("Flight Status")
      .count()
      .orderBy(desc("count"))
  }

  def showDataFrame(df: DataFrame, numRows: Int = 20): Unit = {
    df.show(numRows)
  }
}
