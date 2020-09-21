package com.baidu

import com.github.fommil.netlib.BLAS
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import sun.util.logging.resources.logging

object Word2vec extends Serializable {
  var context: SparkContext = null
  var word2vec = new Word2Vec()
  var model: Word2VecModel = null
  
  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    word2vec.setLearningRate(param.lr)
            .setNumIterations(param.iter)
            .setNumPartitions(param.numPartition)
            .setMinCount(0)
            .setVectorSize(param.dim)

    val word2vecWindowField = word2vec.getClass.getDeclaredField("org$apache$spark$mllib$feature$Word2Vec$$window")
    word2vecWindowField.setAccessible(true)
    word2vecWindowField.setInt(word2vec, param.window)
    
    this
  }
  
  def read(path: String): RDD[Iterable[String]] = {
    context.textFile(path).repartition(200).map(_.split("\\s").toSeq)
  }

//593B482B-0004-D7EA-009AE5F8-00BAE30C#1124363#2_2_2_0_3 1_2_2_1_3 2_2_2_0_3 1_2_2_1_3 2_2_2_0_3 1_2_2_1_3
  def readWithFeat(path: String): RDD[Iterable[String]] = {
    context.textFile(path).repartition(200).map(_.split("#").last.split("\\s"))
  }

  def fit(input: RDD[Iterable[String]]): this.type = {
    model = word2vec.fit(input)
    
    this
  }
  
  def save(outputPath: String): this.type = {
    model.save(context, s"$outputPath.bin")
    this
  }  
  
  def load(path: String): this.type = {
    model = Word2VecModel.load(context, path)
    
    this
  }


  def getVectors = this.model.getVectors

}

