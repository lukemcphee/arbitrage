import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Arbitrage extends App {

  private def vertexId(string: String) = string.hashCode.toLong

  private def vertexIdPair(string: String) = (string.hashCode.toLong, string)

  private val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()

  private val sc = spark.sparkContext


  private val reverseMappings = Seq("BTC", "USD", "EUR").map(entry => vertexId(entry) -> entry).toMap

  private val vertices: RDD[(VertexId, String)] =
    sc.parallelize(Seq(vertexIdPair("BTC"), vertexIdPair("USD"), vertexIdPair("EUR")))

  private val edgesNonNeg: RDD[Edge[Double]] = sc.parallelize(Seq(
    Edge(vertexId("A"), vertexId("B"), 5),
    Edge(vertexId("A"), vertexId("D"), 3),
    Edge(vertexId("B"), vertexId("C"), 3),
    Edge(vertexId("C"), vertexId("D"), -10)
  ))

  private val edgesWithNeg: RDD[Edge[Double]] = sc.parallelize(Seq(
    Edge(vertexId("A"), vertexId("B"), 5),
    Edge(vertexId("A"), vertexId("D"), 3),
    Edge(vertexId("B"), vertexId("C"), 3),
    Edge(vertexId("C"), vertexId("D"), -10),
    Edge(vertexId("D"), vertexId("C"), 4)
  ))


  private val graphWithNoNegativeCycles = Graph(vertices, edgesWithNeg)

  private val bellmanFord = new BellmanFord()

  val relaxedGraph = bellmanFord.bellmanFordBasic(graphWithNoNegativeCycles.vertices.first()._1, graphWithNoNegativeCycles)

  println(relaxedGraph)
}
