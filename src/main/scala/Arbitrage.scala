import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Arbitrage extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()

  val sc = spark.sparkContext

  def vertexId(string: String): Long = {
    string.hashCode.toLong
  }

  def vertexIdPair(string: String): (Long, String) = {
    (string.hashCode.toLong, string)
  }

  val reverseMappings = Seq("BTC", "USD", "EUR").map(entry => vertexId(entry) -> entry).toMap

  val vertices: RDD[(VertexId, String)] =
    sc.parallelize(Seq(vertexIdPair("BTC"), vertexIdPair("USD"), vertexIdPair("EUR")))

  val edges: RDD[Edge[Double]] = sc.parallelize(Seq(
    Edge(vertexId("BTC"), vertexId("USD"), 0),
    Edge(vertexId("USD"), vertexId("EUR"), 0),
    Edge(vertexId("USD"), vertexId("GBP"), 0)
  ))

  val graph = Graph(vertices, edges)

  new BellmanFord(spark).bellmanFordBasic(graph.vertices.first()._1, graph)
}
