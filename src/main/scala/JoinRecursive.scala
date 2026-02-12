import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, sort_array, when, count}

object JoinRecursive {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SimplePageRank")
      .master("local[*]")
      .getOrCreate()


    // 1. Создаем тестовые данные (Edges) - имитация 1 млн строк
    val edges = spark.createDataFrame(Seq(
      ("8911111", "IMEI_1"),
      ("IMEI_1", "8922222"), // Цепочка 1 (Phone -> IMEI -> Phone)
      ("8922222", "IMSI_A"),
      ("IMSI_A", "8911111"), // Продолжение Цепочки 1
      ("8933333", "IMEI_2"),
      ("IMEI_2", "8944444"), // Цепочка 2
      ("8999999", "IMEI_9"), // "Мусорная" связь (нам не нужна),
      ("IMEI_2", "8933333"),
      ("IMEI_2", "IMSI_45")

    )).toDF("src", "dst").cache()

    // 2. Ваши целевые идентификаторы (из которых "растем")
    val targetIds = Seq("8911111", "8933333")

    // 3. Инициализируем результат (сразу записываем, кто чей "корень")
    var result = spark.createDataFrame(targetIds.map(id => (id, id))).toDF("id", "root")

    var frontier = result // Те, кто участвует в поиске на текущем шаге

    for (_ <- 1 to 5) {
      // Ищем соседей в обе стороны
      val nextStep = edges.join(frontier, edges("src") === frontier("id") || edges("dst") === frontier("id"))
        .select(
          when(edges("src") === frontier("id"), edges("dst")).otherwise(edges("src")).as("id"),
          col("root")
        )
        .distinct()

      // Оставляем только тех, кого еще не было в итоговом результате для этого корня
      val newNodes = nextStep.join(result, Seq("id", "root"), "left_anti")

      if (newNodes.isEmpty) {
        // Если новых связей нет - выходим
      } else {
        result = result.union(newNodes)
        frontier = newNodes
      }
    }

    println("Результат поиска цепочек для заданных ID:")
    result.groupBy("root")
      .agg(
        sort_array(collect_list("id")).as("full_chain"),
        count("id").as("total_count"))
      .show(false)

  }
}
