import org.apache.spark.sql.{DataFrame, SparkSession}

class AirlineDataReader(spark: SparkSession, filePath: String) {

  private val df: DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .na.fill("")
      .na.drop()
  }

  def getCleanedDataFrame: DataFrame = df
}

object AirlineReader {
  def apply(spark: SparkSession, filePath: String): AirlineDataReader = new AirlineDataReader(spark, filePath)
}
