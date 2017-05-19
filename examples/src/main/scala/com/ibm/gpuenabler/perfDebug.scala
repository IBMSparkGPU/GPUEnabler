package com.ibm.gpuenabler

import com.ibm.gpuenabler.CUDARDDImplicits._
import com.ibm.gpuenabler.CUDADSImplicits._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object perfDebug {
  def main(args : Array[String]): Unit = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val n: Long = if (args.length > 1) args(1).toLong else 1000000L
    val part = if (args.length > 2) args(2).toInt else 16

    val conf = new SparkConf(false).set("spark.executor.memory", "20g")
    val spark = SparkSession.builder().master(masterURL).appName("test").config(conf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    val ptxURL = this.getClass.getResource("/GpuEnablerExamples.ptx")
    val ptxURL1 = "/GpuEnablerExamples.ptx"
    val mapFunction = new CUDAFunction(
      "multiplyBy2o",
      Array("this"),
      Array("this"),
      ptxURL)

    val dimensions = (size: Long, stage: Int) => stage match {
      case 0 => (64, 256)
      case 1 => (1, 1)
    }
    val reduceFunction = new CUDAFunction(
      "sumlo",
      Array("this"),
      Array("this"),
      ptxURL,
      Seq(),
      Some((size: Long) => 2),
      Some(dimensions))

    val dataRDD = sc.parallelize(1 to n.toInt, part).map(_.toLong).cache()
    dataRDD.count()
    val now = System.nanoTime
    var output: Long = dataRDD.mapExtFunc((x: Long) => 2 * x, mapFunction)
      .reduceExtFunc((x: Long, y: Long) => x + y, reduceFunction)
    val ms = (System.nanoTime - now) / 1000000
    println("RDD Elapsed time: %d ms".format(ms))
    
    println("RDD Output is " + output)

    val dsmapFunction = DSCUDAFunction(
      "multiplyBy2",
      Array("value"),
      Array("value"),
      ptxURL1)

    val dsreduceFunction = DSCUDAFunction(
      "suml",
      Array("value"),
      Array("value"),
      ptxURL1,
      Some((size: Long) => 2),
      Some(dimensions), outputSize=Some(1))

    val data = spark.range(1, n+1, 1, part).cache()
    // val data = dataRDD.toDS().cache()
    data.count()

    val now1 = System.nanoTime
    val mapDS = data.mapExtFunc(2 * _, dsmapFunction).cacheGpu()
    output = mapDS.reduceExtFunc(_ + _, dsreduceFunction)
    val ms1 = (System.nanoTime - now1) / 1000000
    println("DS Elapsed time: %d ms".format(ms1))
    
    mapDS.unCacheGpu()

     println("DS Output is " + output)

    val now3 = System.nanoTime
     data.map(2 * _).reduce(_ + _)
    val ms3 = (System.nanoTime - now3) / 1000000
    println("CPU Elapsed time: %d ms".format(ms3))
   }

}


