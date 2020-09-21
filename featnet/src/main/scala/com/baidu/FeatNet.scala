package com.baidu


import java.io.Serializable
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{EdgeTriplet, Graph, _}
import com.baidu.graph.{GraphOps, EdgeAttr, NodeAttr}

object FeatNet extends Serializable {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName);
  
  var context: SparkContext = null
  var config: Main.Params = null
  var node2id: RDD[(String, Long)] = null
  var indexedEdges: RDD[Edge[EdgeAttr]] = _
  var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  var graph: Graph[NodeAttr, EdgeAttr] = _
  var randomWalkPaths: RDD[(Long, (ArrayBuffer[Long], ArrayBuffer[String], Array[Long]))] = null
  var node2feat: RDD[(String, String)] = null
  val sep = "\u0001"

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param
    
    this
  }

  def paddingGraphAndSave(): this.type = {
    val rddFeat1: RDD[(String, String)] = context.textFile(config.featPath1).map { line =>
      val parts = line.split("\\s")
      Try {
        (parts.head, parts.last)
      }.getOrElse(null)
    }.filter(_ != null)

    val rddFeat2: RDD[(String, String)] = context.textFile(config.featPath2).map { line =>
      val parts = line.split("\\s")
      Try {
        (parts.head, parts.last)
      }.getOrElse(null)
    }.filter(_ != null)

    val rddGraph: RDD[(String, String)] = context.textFile(config.input).map { line =>
      val parts = line.split("\\s")
      Try {
        (parts.head, parts.last)
      }.getOrElse(null)
    }.filter(_ != null)

    rddGraph.join(rddFeat1).map { case (src, (dst: String, srcFeat: String)) =>
      try {
        val (new_src: String) = src + sep + srcFeat
        (dst, new_src)
      } catch {
        case e: Exception => null
      }
    }.filter(_ != null).join(rddFeat2).map{ case (dst, (new_src: String, dstFeat: String)) =>
      try {
        val (new_dst: String) = dst + sep + dstFeat
        (new_src, new_dst)
      } catch {
        case e: Exception => null
      }
    }.filter(_ != null).map { case (new_src, new_dst) => s"$new_src $new_dst" }
      .distinct()
      .repartition(200)
      .saveAsTextFile(s"${config.output}.padd")

    this
  }

  
  def embedding(): this.type = {
    val randomPaths = randomWalkPaths.map { case (vertexId, (pathBuffer, featBuffer, nbr)) =>
      Try(pathBuffer.map(_.toString).toIterable).getOrElse(null)
    }.filter(_!=null)
    Word2vec.setup(context, config).fit(randomPaths)
    this
  }

  def embeddingWithFeat(): this.type = {
    val randomPaths = randomWalkPaths.map { case (vertexId, (pathBuffer, featBuffer, nbr)) =>
      Try(featBuffer.map(_.toString).toIterable).getOrElse(null)
    }.filter(_!=null)
    Word2vec.setup(context, config).fit(randomPaths)
    this
  }

  def save(): this.type = {
    this.saveRandomPath()
        .saveModel()
        .saveVectors()
  }
  
  def saveRandomPath(): this.type = {

    if (this.node2id != null) {
      val id2Node = this.node2id.map { case (strNode, index) =>
        (index, strNode)
      }

      val walkPath = randomWalkPaths.map{ case (vertexId: Long, info:(ArrayBuffer[Long], ArrayBuffer[String], Array[Double])) =>
        (vertexId, (info._1, info._2))
      }

      val rddRawRes = walkPath
        .join(id2Node).map { case (vertexId: Long, (info: (ArrayBuffer[Long], ArrayBuffer[String]), strNode: String)) =>
        try {
          val (pathBuffer, featBuffer) = info
          (strNode + sep + vertexId.toString(), (pathBuffer, featBuffer))
        } catch {
          case e: Exception => null
        }
      }.filter(x => x != null && x._1.replaceAll("\\s", "").length > 0)

      rddRawRes.map { case (strNode:String, (pathBuffer:ArrayBuffer[Long], featBuffer:ArrayBuffer[String])) =>
          Try(strNode + sep + pathBuffer.mkString(" ")).getOrElse(null)
        }
        .repartition(200)
        .saveAsTextFile(config.output)


      rddRawRes.map { case(strNode:String, (pathBuffer:ArrayBuffer[Long], featBuffer:ArrayBuffer[String])) =>
          Try(strNode + sep + featBuffer.mkString(" ")).getOrElse(null)
        }
        .repartition(200)
        .saveAsTextFile(config.featWalkPath)

      rddRawRes.map { case (strNode:String, (pathBuffer:ArrayBuffer[Long], featBuffer:ArrayBuffer[String]))=>
        (strNode.split(sep).head, (pathBuffer, featBuffer))}
        .join(this.node2feat).map{case (strNode:String, ((pathBuffer, featBuffer), nodeFeat:String)) =>
        Try(strNode + sep + nodeFeat + sep + featBuffer.mkString(" ")).getOrElse(null)
      }
        .repartition(200)
        .saveAsTextFile(config.featWalkPath + '2')
    }
    this
  }
  
  def saveModel(): this.type = {
    Word2vec.save(config.output)
    this
  }
  
  def saveVectors(): this.type = {
    val node2vector = context.parallelize(Word2vec.getVectors.toList)
            .map { case (nodeId, vector) =>
              (nodeId.toLong, vector.mkString(","))
            }
    
    if (this.node2id != null) {
      val id2Node = this.node2id.map{ case (strNode, index) =>
        (index, strNode)
      }
      
      node2vector.join(id2Node)
              .map { case (nodeId, (vector, name)) => s"$name\t$vector" }
              .repartition(200)
              .saveAsTextFile(s"${config.output}.emb")
    } else {
      node2vector.map { case (nodeId, vector) => s"$nodeId\t$vector" }
              .repartition(200)
              .saveAsTextFile(s"${config.output}.emb")
    }
    this
  }

  def saveVectorsWithFeat(): this.type = {

    val node2vector = context.parallelize(Word2vec.getVectors.toList)
      .map { case (nodeFeat, vector) =>
        (nodeFeat, vector.mkString(","))
      }

    if (this.node2feat != null) {
      val feat2Node = this.node2feat.map{ case (strNode, feat) =>
        (feat, strNode)
      }

      node2vector.join(feat2Node)
        .map { case (feat, (vector, strNode)) => s"$strNode\t$feat\t$vector" }
        .distinct()
        .repartition(200)
        .saveAsTextFile(s"${config.output}.emb")
    }
    this
  }


  def cleanup(): this.type = {
    node2id.unpersist(blocking = false)
    indexedEdges.unpersist(blocking = false)
    indexedNodes.unpersist(blocking = false)
    graph.unpersist(blocking = false)
    randomWalkPaths.unpersist(blocking = false)
    
    this
  }


  def createNode2Id[T <: Any](triplets: RDD[(String, String, T)]) = triplets.flatMap { case (src, dst, weight) =>
    Try(Array(src, dst)).getOrElse(Array.empty[String])
  }.distinct().zipWithIndex().cache()

  //rev
  def initTransitionProbWithFeat(): this.type = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)

    graph = Graph(indexedNodes, indexedEdges)
      .mapVertices[NodeAttr] { case (vertexId, clickNode) =>
      val (j, q) = GraphOps.setupAlias(clickNode.neighbors)
      val nextNodeIndex = GraphOps.drawAlias(j, q)
      clickNode.path = Array(vertexId, clickNode.neighbors(nextNodeIndex)._1)

      clickNode
    }
      .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId, edgeTriplet.srcAttr.neighbors, edgeTriplet.dstAttr.neighbors)
        edgeTriplet.attr.J = j
        edgeTriplet.attr.q = q
        edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)
        edgeTriplet.attr.dstNeighborFeat = edgeTriplet.dstAttr.neighborsFeat
        edgeTriplet.attr
      }.cache


    this
  }

  def randomWalkWithFeat(): this.type = {
    val edge2attr = graph.triplets.map { edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }.repartition(200).cache
    edge2attr.first

    val tmp = graph.vertices.map { case (nodeId, clickNode) =>
      (nodeId, clickNode.path, clickNode.neighbors)
    }

    for (iter <- 0 until config.numWalks) {
      var prevWalk: RDD[(Long, (ArrayBuffer[Long], ArrayBuffer[String], Array[Long]))] = null
      var randomWalk = graph.vertices.map { case (nodeId, clickNode) =>
        val pathBuffer = new ArrayBuffer[Long]()
        val featBuffer = new ArrayBuffer[String]()
        pathBuffer.append(clickNode.path:_*)

        val neighbors_ = for(elem <- clickNode.neighbors) yield elem._1
        val nodeIdx = neighbors_.indexOf(clickNode.path.last)
        featBuffer.append(clickNode.neighborsFeat(nodeIdx))

        (nodeId, (pathBuffer, featBuffer, neighbors_))
      }.cache

      var activeWalks = randomWalk.first
      graph.unpersist(blocking = false)
      graph.edges.unpersist(blocking = false)

      for (walkCount <- 0 until config.walkLength) {
        prevWalk = randomWalk
        randomWalk = randomWalk.map { case (srcNodeId, (pathBuffer, featBuffer, srcNeighbors)) =>
          val prevNodeId = pathBuffer(pathBuffer.length - 2)
          val currentNodeId = pathBuffer.last

          (s"$prevNodeId$currentNodeId", (srcNodeId, (pathBuffer, featBuffer, srcNeighbors)))
        }.join(edge2attr).map { case (edge, ((srcNodeId, (pathBuffer, featBuffer, srcNeighbors)), attr)) =>
          try {
            val nextNodeIndex = GraphOps.drawAlias(attr.J, attr.q)
            val nextNodeId = attr.dstNeighbors(nextNodeIndex)
            val nextNodeFeat = attr.dstNeighborFeat(nextNodeIndex)

            //weight
            var w = 0
            if (nextNodeId == srcNodeId){w = 0}
            else if (srcNeighbors.indexOf(nextNodeId) != -1){w = 1}
            else{w = 2}

            pathBuffer.append(nextNodeId)
            //featBuffer.append(nextNodeFeat + ":" + w.toString)
            featBuffer.append(nextNodeFeat)
            (srcNodeId, (pathBuffer, featBuffer, srcNeighbors))
          } catch {
            case e: Exception => throw new RuntimeException(e.getMessage)
          }
        }.cache

        activeWalks = randomWalk.first()
        prevWalk.unpersist(blocking=false)
      }


      if (randomWalkPaths != null) {
        val prevRandomWalkPaths = randomWalkPaths
        randomWalkPaths = randomWalkPaths.union(randomWalk).cache()
        randomWalkPaths.first
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        randomWalkPaths = randomWalk
      }
    }

    this
  }

  //rev
  def loadWithFeat(): this.type = {
    val bcMaxDegree = context.broadcast(config.degree)
    val bcEdgeCreator = config.directed match {
      //case true => context.broadcast(GraphOps.createDirectedEdge)
      //case false => context.broadcast(GraphOps.createUndirectedEdgeWithFeat)
      case false => context.broadcast(GraphOps.createTicEdgeWithFeat)
    }

    val inputTriplets: RDD[(Long, Long, String, String, Double)] = config.indexed match {
      //case true => readIndexedGraph(config.input)
        case false => indexingGraphWithFeat(config.input)
    }
    val rdd_tmp = inputTriplets.flatMap { case (srcId, dstId, srcFeat, dstFeat, weight) =>
      bcEdgeCreator.value.apply(srcId, srcFeat, dstId, dstFeat, weight)
        //rdd(Array((srcId, Array((dstId, (dstFeat, weight))))) -> flatmap)
    }.reduceByKey(_++_)

    rdd_tmp.map{case (srcId, neighbors: Array[(VertexId, (String, Double))]) =>
      srcId.toString + " " + sep + " " + neighbors.mkString(" ")
    }

    indexedNodes = rdd_tmp.map { case (srcId, neighbors: Array[(VertexId, (String, Double))]) =>
      var neighbors_ = neighbors
      if (neighbors_.length > bcMaxDegree.value) {
        neighbors_ = neighbors.sortWith{ case (left, right) => left._2._2 > right._2._2 }.slice(0, bcMaxDegree.value)
      }
      val neighborsId = for(elem <- neighbors_) yield (elem._1, elem._2._2)
      val neighborsWithFeat = for(elem <- neighbors_) yield elem._2._1
      (srcId, NodeAttr(neighbors = neighborsId.distinct, neighborsFeat = neighborsWithFeat))
    }.repartition(200).cache

    indexedEdges = indexedNodes.flatMap {
      case (srcId, clickNode) =>
      clickNode.neighbors.map {case (dstId, weight) => Edge(srcId, dstId, EdgeAttr())}
    }.repartition(200).cache

    this
  }

  def loadNode2feat(path: String):this.type  = {
    this.node2feat = context.textFile(path).map{line=>
      (line.split(sep)(0),line.split(sep)(1))
    }.repartition(200).cache()
    this
  }

  def indexingGraphWithFeat(rawTripletPath: String): RDD[(Long, Long, String, String, Double)] = {
    val rawLines = context.textFile(rawTripletPath).map { triplet =>
      val parts = triplet.split("\\s")
      val kvSrc = parts.head.split(sep)
      val kvDst = parts.last.split(sep)
      Try {
        (kvSrc.head, kvDst.head, kvSrc.last, kvDst.last, Try(parts.last.toDouble).getOrElse(1.0))
      }.getOrElse(null)
    }.filter(_!=null)

    this.node2feat = rawLines.map{ case (src, dst, srcFeat, dstFeat, weight) =>
      (src, srcFeat)
    }.repartition(200).cache()

    val rawEdges = context.textFile(rawTripletPath).map { triplet =>
      val parts = triplet.split("\\s")
      val kvSrc = parts.head.split(sep)
      val kvDst = parts.last.split(sep)
      Try {
        (kvSrc.head, kvDst.head, Try(parts.last.toDouble).getOrElse(1.0))
      }.getOrElse(null)
    }.filter(_!=null)

    this.node2id = createNode2Id(rawEdges)
    this.node2id.repartition(200).saveAsTextFile(config.featWalkPath + ".node2id")

    val rdd_tmp = rawLines.map { case (src, dst, srcFeat, dstFeat, weight) =>
      (src, (dst, srcFeat, dstFeat, weight))
    }

    val j1 = rdd_tmp.join(node2id).map { case (src: String, (edge: (String, String, String, Double), srcIndex: Long)) =>
      try {
        val (dst, srcFeat, dstFeat, weight) = edge
        (dst, (srcIndex, srcFeat, dstFeat, weight))
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null)

    j1.map{ case (dst, (srcIndex, srcFeat, dstFeat, weight)) =>
      dst + " " + sep + " " + srcIndex.toString + ' ' + srcFeat + " " +dstFeat
    }
      .repartition(200)
      .saveAsTextFile(config.featWalkPath + ".j2")

      val j2 = j1.join(node2id).map { case (dst: String, (edge: (Long, String, String, Double), dstIndex: Long)) =>
      try {
        val (srcIndex, srcFeat, dstFeat, weight) = edge
        (srcIndex, dstIndex, srcFeat, dstFeat, weight)
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null)

    j2
  }
}
