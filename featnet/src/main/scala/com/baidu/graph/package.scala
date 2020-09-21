package com.baidu

import java.io.Serializable

package object graph {
  case class NodeAttr(var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
                      var neighborsFeat:  Array[String] = Array.empty[String],
                      var path: Array[Long] = Array.empty[Long]) extends Serializable  //path of (srcID , next dstID)

  case class EdgeAttr(var dstNeighbors: Array[Long] = Array.empty[Long],
                      var dstNeighborFeat: Array[String] = Array.empty[String],
                      var J: Array[Int] = Array.empty[Int],
                      var q: Array[Double] = Array.empty[Double]) extends Serializable
}
