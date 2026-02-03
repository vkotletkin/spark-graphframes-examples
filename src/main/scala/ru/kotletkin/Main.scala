import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkStableApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")


    val spark = SparkSession.builder()
      .appName("StableSparkApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Чтение CSV с заголовками
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input.csv") // путь к файлу

    // Пример обработки: группировка и агрегация
    val result = df
      .groupBy($"category") // группировка по категории
      .agg(
        count($"id").as("total_count"),
        avg($"price").as("avg_price"),
        sum($"amount").as("total_amount")
      )
      .orderBy($"total_amount".desc) // сортировка

    // Вывод схемы и данных
    df.printSchema()
    result.show(10, truncate = false)

    // Запись результата в Parquet
    result.write
      .mode("overwrite")
      .csv("output.csv")

    spark.stop()
  }
}