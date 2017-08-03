package com.ibm.gpuenabler

import com.ibm.gpuenabler.CUDARDDImplicits._
import com.ibm.gpuenabler.CUDADSImplicits._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object perfDebug {
  def timeit(msg: String, code: => Any): Any ={
    val now1 = System.nanoTime
    code
    val ms1 = (System.nanoTime - now1) / 1000000
    println("%s Elapsed time: %d ms".format(msg, ms1))
  }

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
    val loadFunction = new CUDAFunction(
      "load",
      Array("this"), Seq(),
      ptxURL)

    val dataRDD = sc.parallelize(1 to n.toInt, part).map(_.toLong).cache().cacheGpu()
    dataRDD.count()
    // Load the data to GPU
    dataRDD.reduceExtFunc((x1, x2) => x2 , loadFunction)

    timeit("RDD: All cached", {
      val mapRDD = dataRDD.mapExtFunc((x: Long) => 2 * x, mapFunction).cacheGpu()
      val output: Long = mapRDD.reduceExtFunc((x: Long, y: Long) => x + y, reduceFunction)
      mapRDD.unCacheGpu()
      println("RDD Output is " + output)
    })

    val dsmapFunction = DSCUDAFunction(
      "multiplyBy2",
      Array("value"),
      Array("value"),
      ptxURL1)

    val dimensions2 = (size: Long, stage: Int) => stage match {
      case 0 => (64, 256, 1, 1, 1, 1)
      case 1 => (1, 1, 1, 1, 1, 1)
    }

    val gpuParams = gpuParameters(dimensions2)

    val dsreduceFunction = DSCUDAFunction(
      "suml",
      Array("value"),
      Array("value"),
      ptxURL1,
      Some((size: Long) => 2),
      Some(gpuParams), outputSize=Some(1))

    val rd = spark.range(1, n+1, 1, part).cache()
    rd.count()

    val data = rd.cacheGpu(true)
    // Load the data to GPU
    timeit("Data load in GPU", { data.loadGpu()})

    timeit("DS: All cached", {
      val mapDS = data.mapExtFunc(2 * _, dsmapFunction).cacheGpu()
      val mapDS1 = mapDS.mapExtFunc(2 * _, dsmapFunction).cacheGpu()
      val mapDS2 = mapDS1.mapExtFunc(2 * _, dsmapFunction).cacheGpu()
      val output = mapDS2.reduceExtFunc(_ + _, dsreduceFunction)
      mapDS.unCacheGpu()
      mapDS1.unCacheGpu()
      mapDS2.unCacheGpu()
      println("Output is " + output)
    })
    data.unCacheGpu()

    val data111 = rd.cacheGpu(true)
    // Load the data to GPU
    timeit("Data load in GPU", {data111.loadGpu() })

    timeit("DS: All cached GPUONLY", { 
      val mapDS123 = data111.mapExtFunc(2 * _, dsmapFunction).cacheGpu(true)
      val output = mapDS123.reduceExtFunc(_ + _, dsreduceFunction)
      mapDS123.unCacheGpu()
      println(s"Output is $output")
    })
 
    data111.unCacheGpu()

    val data1 = rd
    // Load the data to GPU
    timeit("Data load in GPU", {data1.loadGpu() })

    timeit("DS: No Cache", {
      val mapDS1 = data1.mapExtFunc(2 * _, dsmapFunction) 
      val output = mapDS1.reduceExtFunc(_ + _, dsreduceFunction)
      println("Output is " + output)
    })
    data1.unCacheGpu()

    timeit("DS: CPU", {
      val output = data.map(2 * _).reduce(_ + _)
      println("Output is " + output)
    })
  }
}


