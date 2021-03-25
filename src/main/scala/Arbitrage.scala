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

}
