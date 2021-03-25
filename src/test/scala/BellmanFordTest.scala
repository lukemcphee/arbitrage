import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import com.typesafe.scalalogging.Logger

class BellmanFordTest extends FunSuite {

  val logger = Logger[BellmanFordTest]

  private def vertexId(string: String) = string.hashCode.toLong

  private def vertexIdPair(string: String) = (string.hashCode.toLong, string)

  private val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()

  private val sc = spark.sparkContext

  private val vertices: RDD[(VertexId, String)] =
    sc.parallelize(Seq(vertexIdPair("A"), vertexIdPair("B"), vertexIdPair("C"), vertexIdPair("D")))

  private val bellman = new BellmanFord()


  test("Graph without negative cycle doesn't change with extra relaxations") {

    val edgesNonNeg: RDD[Edge[Double]] = sc.parallelize(Seq(
      Edge(vertexId("A"), vertexId("B"), 5),
      Edge(vertexId("A"), vertexId("D"), 3),
      Edge(vertexId("B"), vertexId("C"), 3),
      Edge(vertexId("C"), vertexId("D"), -10)
    ))


    val graphWithNoNegativeCycles = Graph(vertices, edgesNonNeg)
    val sourceVertexId = graphWithNoNegativeCycles.vertices.first()._1
    val reduction = bellman.relaxNonSpark(sourceVertexId, graphWithNoNegativeCycles)
    val additionalReduction = bellman.relaxOnce(edgesNonNeg.collect(), reduction)
    logger.info(s"Reduction distances: ${reduction.distances}")
    logger.info(s"Reduction previous map: ${reduction.previous}")
    assert(reduction === additionalReduction)
  }

  test("Graph containing negative cycle does change with extra relaxations") {
    val edgesWithNegativeCycle: RDD[Edge[Double]] = sc.parallelize(Seq(
      Edge(vertexId("A"), vertexId("B"), 5),
      Edge(vertexId("A"), vertexId("D"), 3),
      Edge(vertexId("B"), vertexId("C"), 3),
      Edge(vertexId("C"), vertexId("D"), -10),
      Edge(vertexId("D"), vertexId("C"), 4)
    ))

    val graphWithNegativeCycles = Graph(vertices, edgesWithNegativeCycle)
    val sourceVertexId = graphWithNegativeCycles.vertices.first()._1
    val reduction = bellman.relaxNonSpark(sourceVertexId, graphWithNegativeCycles)
    val additionalReduction = bellman.relaxOnce(edgesWithNegativeCycle.collect(), reduction)
    assert(reduction !== additionalReduction)
  }
}
