import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession

case class Reduction(distances: Map[VertexId, Double], previous: Map[VertexId, VertexId])


class BellmanFord(spark: SparkSession) {

  def bellmanFordBasic(source: VertexId, graph: Graph[String, Double]) = {
    val initDistances = Map(source -> 0d)
    // this contains a map of each vertex, to which vertex it was last updated from,
    // we'll use this to backtrack and find what each path actually is
    val initPrevious = Map[VertexId, VertexId]()

    val pass = graph
      .edges
      .collect()
      .foldLeft(Reduction(initDistances, initPrevious))((maps, edge) => {
        val distances = maps.distances
        val previous = maps.previous
        (distances.get(edge.srcId), distances.get(edge.dstId)) match {
          case (None, _) => Reduction(distances, previous)
          case (Some(distanceToSrc), None) =>
            Reduction(distances + (edge.dstId -> edge.attr),
              previous + (edge.dstId -> edge.srcId))
          case (Some(distanceToSrc), Some(distanceToDst)) if distanceToSrc + edge.attr < distanceToDst => {
            Reduction(distances + (edge.dstId -> edge.attr),
              previous + (edge.dstId -> edge.srcId))
          }
        }
      })
    print(pass)
  }



}
