import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark 4.1.0 Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val v = spark.createDataFrame((0L until 30L).map(id => (id, s"v$id"))).toDF("id", "name")

    // Build edges to create a hierarchical structure:
    // Core (k=5): vertices 0-4 - fully connected
    // Next layer (k=3): vertices 5-14 - each connects to multiple core vertices
    // Outer layer (k=1): vertices 15-29 - sparse connections
    val coreEdges = for {
      i <- 0 until 5
      j <- (i + 1) until 5
    } yield (i.toLong, j.toLong)

    val midLayerEdges = Seq(
      (5L, 0L),
      (5L, 1L),
      (5L, 2L), // Connect to core
      (6L, 0L),
      (6L, 1L),
      (6L, 3L),
      (7L, 1L),
      (7L, 2L),
      (7L, 4L),
      (8L, 0L),
      (8L, 3L),
      (8L, 4L),
      (9L, 1L),
      (9L, 2L),
      (9L, 3L),
      (10L, 0L),
      (10L, 4L),
      (11L, 2L),
      (11L, 3L),
      (12L, 1L),
      (12L, 4L),
      (13L, 0L),
      (13L, 2L),
      (14L, 3L),
      (14L, 4L))

    val outerEdges = Seq(
      (15L, 5L),
      (16L, 6L),
      (17L, 7L),
      (18L, 8L),
      (19L, 9L),
      (20L, 10L),
      (21L, 11L),
      (22L, 12L),
      (23L, 13L),
      (24L, 14L),
      (25L, 15L),
      (26L, 16L),
      (27L, 17L),
      (28L, 18L),
      (29L, 19L))

    val allEdges = coreEdges ++ midLayerEdges ++ outerEdges

    val e = spark.createDataFrame(allEdges).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore
      .setCheckpointInterval(5)
      .setUseLocalCheckpoints(true)
      .setIntermediateStorageLevel(StorageLevel.MEMORY_AND_DISK)
      .run()

    val coreOnly = result.filter($"kcore" > 5)
    coreOnly.show()
  }
}