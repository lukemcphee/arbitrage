import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.annotation.tailrec

case class Reduction(distances: Map[VertexId, Double], previous: Map[VertexId, VertexId])


class BellmanFord {

  def relaxNonSpark(source: VertexId, graph: Graph[String, Double]) = {
    val verticesCount = graph.vertices.count()
    val edges = graph
      .edges
      .collect()

    @tailrec
    def relax(passes: Long, reduction: Reduction): Reduction =
      if (passes == 0) {
        reduction
      }
      else {
        relax(passes - 1, relaxOnce(edges, reduction))
      }

    relax(verticesCount, Reduction(Map(source -> 0d), Map[VertexId, VertexId]()))
  }

  def relaxOnce(edges: Array[Edge[Double]], reduction: Reduction) = {
    edges
      .foldLeft(reduction)((reduction, edge) => {
        val distances = reduction.distances
        val previous = reduction.previous
        (distances.get(edge.srcId), distances.get(edge.dstId)) match {
          case (None, _) => reduction
          case (Some(distanceToSrc), None) =>
            Reduction(distances + (edge.dstId -> (distanceToSrc + edge.attr)), previous + (edge.dstId -> edge.srcId))
          case (Some(distanceToSrc), Some(distanceToDst)) =>
            if (distanceToSrc + edge.attr < distanceToDst)
              Reduction(distances + (edge.dstId -> (distanceToSrc + edge.attr)), previous + (edge.dstId -> edge.srcId))
            else
              reduction
        }
      })
  }


}
