import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import org.graphframes.GraphFrame

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark 4.1.0 Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //    val v = Seq(
    //      ("email:ivan@mail.ru", "email"),
    //      ("cookie:abc123", "cookie"),
    //      ("phone:7999123", "phone"),
    //      ("user_id:555", "internal_id"),
    //      ("email:petr@mail.ru", "email"),
    //      ("cookie:xyz789", "cookie")
    //    ).toDF("id", "type")


    val e = Seq(
      ("ivan@mail.ru", "abc123"), // Иван зашел с кукой abc
      ("abc123", "7999123"), // С этой же куки подтвердили телефон
      ("7999123", "555"), // Телефон привязан к профилю 555
      ("petr@mail.ru", "xyz789") // Петр использует свои девайсы
    ).toDF("src", "dst")

    val v = e.select($"src".as("id")).union(e.select($"dst".as("id"))).distinct()

    val g = GraphFrame(v, e)

    val components = g.connectedComponents
      .setUseLocalCheckpoints(true)
      .run()

    val groupped = components.groupBy("component")
      .agg(
        collect_list($"id").as("all_identifiers")
      )

    groupped.show(false)

  }
}