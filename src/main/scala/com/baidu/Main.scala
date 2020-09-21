package com.baidu

import java.io.Serializable
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import com.baidu.lib.AbstractParams

object Main {
  object Command extends Enumeration {
    type Command = Value
    val padding, featNet, randomwalk, embedding = Value
  }
  import Command._

  case class Params(iter: Int = 10,
                    lr: Double = 0.025,
                    numPartition: Int = 10,
                    dim: Int = 128,
                    window: Int = 10,
                    walkLength: Int = 80,
                    numWalks: Int = 10,
                    p: Double = 1.0,
                    q: Double = 1.0,
                    weighted: Boolean = true,
                    directed: Boolean = false,
                    degree: Int = 50,
                    indexed: Boolean = true,
                    nodePath: String = null,
                    input: String = null,
                    output: String = null,
                    featPath1: String = null,
                    featPath2: String = null,
                    featWalkPath: String = null,
                    cmd: Command = Command.featNet) extends AbstractParams[Params] with Serializable
  val defaultParams = Params()
  
  val parser = new OptionParser[Params]("FeatNet") {
    head("Main")
    opt[Int]("walkLength")
            .text(s"walkLength: ${defaultParams.walkLength}")
            .action((x, c) => c.copy(walkLength = x))
    opt[Int]("numWalks")
            .text(s"numWalks: ${defaultParams.numWalks}")
            .action((x, c) => c.copy(numWalks = x))
    opt[Double]("p")
            .text(s"return parameter p: ${defaultParams.p}")
            .action((x, c) => c.copy(p = x))
    opt[Double]("q")
            .text(s"in-out parameter q: ${defaultParams.q}")
            .action((x, c) => c.copy(q = x))
    opt[Boolean]("weighted")
            .text(s"weighted: ${defaultParams.weighted}")
            .action((x, c) => c.copy(weighted = x))
    opt[Boolean]("directed")
            .text(s"directed: ${defaultParams.directed}")
            .action((x, c) => c.copy(directed = x))
    opt[Int]("degree")
            .text(s"degree: ${defaultParams.degree}")
            .action((x, c) => c.copy(degree = x))
    opt[Boolean]("indexed")
            .text(s"Whether nodes are indexed or not: ${defaultParams.indexed}")
            .action((x, c) => c.copy(indexed = x))
    opt[String]("nodePath")
            .text("Input node2index file path: empty")
            .action((x, c) => c.copy(nodePath = x))
    opt[String]("input")
            .required()
            .text("Input edge file path: empty")
            .action((x, c) => c.copy(input = x))
    opt[String]("output")
            .required()
            .text("Output path: empty")
            .action((x, c) => c.copy(output = x))
    opt[String]("feat1")
            .text("File path of Type I vertex: empty")
            .action((x, c) => c.copy(featPath1 = x))
    opt[String]("feat2")
            .text("File path of Type II vertex: empty")
            .action((x, c) => c.copy(featPath2 = x))
    opt[String]("feat_walk")
            .text("feat_path file path: empty")
            .action((x, c) => c.copy(featWalkPath = x))
    opt[String]("dim")
          .text("w2v output dim: empty")
          .action((x, c) => c.copy(dim = x.toShort))
    opt[String]("cmd")
            .required()
            .text(s"command: ${defaultParams.cmd.toString}")
            .action((x, c) => c.copy(cmd = Command.withName(x)))
    note(
        .stripMargin +
              s"|   --lr ${defaultParams.lr}" +
              s"|   --iter ${defaultParams.iter}" +
              s"|   --numPartition ${defaultParams.numPartition}" +
              s"|   --dim ${defaultParams.dim}" +
              s"|   --window ${defaultParams.window}" +
              s"|   --input <path>" +
              s"|   --node <nodeFilePath>" +
              s"|   --output <path>"
    )
  }
  
  def main(args: Array[String]) = {
    parser.parse(args, defaultParams).map { param =>
      val conf = new SparkConf().setAppName("FeatNet v0.1.0")
      val context: SparkContext = new SparkContext(conf)

      FeatNet.setup(context, param)
      
      param.cmd match {
        case Command.padding => FeatNet.paddingGraphAndSave()
        case Command.randomwalk => FeatNet.loadWithFeat()
                                           .initTransitionProbWithFeat()
                                           .randomWalkWithFeat()
                                           .saveRandomPath()

      }
    } getOrElse {
      sys.exit(1)
    }
  }
}
