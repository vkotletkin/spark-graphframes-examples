import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, collect_list}
import org.graphframes.GraphFrame

object ConnectedComponents {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SimplePageRank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val targetIds = Seq("8911111", "8933333", "IMSI_A")
    val targetsDF = targetIds.toDF("target_id")
    val targets = targetIds.toDF("id")

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

    val vertices = edges.select("src").union(edges.select("dst")).distinct()
      .withColumnRenamed("src", "id") // или обобщенное имя

    val g = GraphFrame(vertices, edges)
    val cc = g.connectedComponents.setUseLocalCheckpoints(true).run().cache()

    // взяли components с нашими id
    val componentsOfInterest = cc.join(broadcast(targets), "id")
      .select("component").distinct()

    // взяли target_ids от них
    val componentWithTargets = cc.join(broadcast(targetsDF), cc("id") === targetsDF("target_id"))
      .groupBy("component")
      .agg(collect_list("target_id").as("targets_in_component"))

    val result = cc.join(componentsOfInterest, "component")
      .groupBy("component")
      .agg(collect_list("id").as("full_chain"))
      .join(componentWithTargets, Seq("component"), "left")

    result.show(false)
  }
}