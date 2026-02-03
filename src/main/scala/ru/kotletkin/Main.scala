package ru.kotletkin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    // Считаем сумму чисел от 1 до 100
    val df = spark.range(1, 5) // создает колонку "id"
      .withColumn("status", lit("active")) // добавляет колонку со строкой

    df.show()
    spark.stop()
  }
}
