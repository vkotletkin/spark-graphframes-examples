import org.apache.spark.sql.SparkSession

object SimplePageRank {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SimplePageRank")
      .master("local[*]")
      .getOrCreate()

    // Create a Vertex DataFrame with unique ID column "id"
    val v = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30)
    )).toDF("id", "name", "age")

    // Create an Edge DataFrame with "src" and "dst" columns
    val e = spark.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow")
    )).toDF("src", "dst", "relationship")
    // Create a GraphFrame

    import org.graphframes.GraphFrame

    val g = GraphFrame(v, e)

    // Query: Get in-degree of each vertex.
    g.inDegrees.show()

    // Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    // Run PageRank algorithm, and show results.
    val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
    results.vertices.select("id", "pagerank").show()

  }
}
