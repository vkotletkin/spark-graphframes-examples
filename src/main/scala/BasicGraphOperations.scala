import org.apache.spark.sql.SparkSession
import org.graphframes.{GraphFrame, examples}

object BasicGraphOperations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark 4.1.0 Test")
      .master("local[*]")
      .getOrCreate()

    val g: GraphFrame = examples.Graphs.friends // get example graph

    // Display the vertex and edge DataFrames
    g.vertices.show()
    // +--+-------+---+
    // |id|   name|age|
    // +--+-------+---+
    // | a|  Alice| 34|
    // | b|    Bob| 36|
    // | c|Charlie| 30|
    // | d|  David| 29|
    // | e| Esther| 32|
    // | f|  Fanny| 36|
    // | g|  Gabby| 60|
    // +--+-------+---+

    g.edges.show()
    // +---+---+------------+
    // |src|dst|relationship|
    // +---+---+------------+
    // |  a|  b|      friend|
    // |  b|  c|      follow|
    // |  c|  b|      follow|
    // |  f|  c|      follow|
    // |  e|  f|      follow|
    // |  e|  d|      friend|
    // |  d|  a|      friend|
    // |  a|  e|      friend|
    // +---+---+------------+

    // import Spark SQL package
    import org.apache.spark.sql.DataFrame

    // Get a DataFrame with columns "id" and "inDeg" (in-degree)
    val vertexInDegrees: DataFrame = g.inDegrees
    vertexInDegrees.show()

    // Find the youngest user's age in the graph.
    // This queries the vertex DataFrame.
    g.vertices.groupBy().min("age").show()

    // Count the number of "follows" in the graph.
    // This queries the edge DataFrame.
    val numFollows = g.edges.filter("relationship = 'follow'").count()

    println(numFollows)

    import spark.implicits._

    val matureGraph = g.filterEdges($"relationship" === "follow");

    matureGraph.vertices.show()
  }
}