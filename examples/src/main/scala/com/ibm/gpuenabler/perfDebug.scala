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

/*
    val dataRDD = sc.parallelize(1 to n.toInt, part).map(_.toLong).cache().cacheGpu()
    dataRDD.count()
    // Load the data to GPU
    dataRDD.reduceExtFunc((x1, x2) => x2 , loadFunction)

    val now = System.nanoTime
    var output: Long = dataRDD.mapExtFunc((x: Long) => 2 * x, mapFunction).cacheGpu()
      .reduceExtFunc((x: Long, y: Long) => x + y, reduceFunction)
    val ms = (System.nanoTime - now) / 1000000
    println("RDD Elapsed time: %d ms".format(ms))
    
    println("RDD Output is " + output)
*/

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
    // Load the data to GPU and cache it
    data.loadGpu()
    val rd = data
    println(" ==== ")

    val now1 = System.nanoTime
    val mapDS = data.mapExtFunc(2 * _, dsmapFunction).cacheGpu()
    var output = mapDS.reduceExtFunc(_ + _, dsreduceFunction)
    val ms1 = (System.nanoTime - now1) / 1000000
    println("DS Elapsed time: %d ms".format(ms1))
    println("DS Output is " + output)

    mapDS.unCacheGpu()
    println("all cached DS Output is " + output)

    val data111 = rd.cacheGpu(true)
    data111.reduceExtFunc((x1, x2) => x2 , dsloadFunction)
    println(" ==== ")
    timeit("DS GPUONLY", { 
      val mapDS123 = data111.mapExtFunc(2 * _, dsmapFunction).cacheGpu(true)
      output = mapDS123.reduceExtFunc(_ + _, dsreduceFunction)
      println("GPUONLY cached DS Output is " + output)
    })

    val data1 = rd
    data1.reduceExtFunc((x1, x2) => x2 , dsloadFunction)
    println(" ==== ")

    val now11 = System.nanoTime
    val mapDS1 = data1.mapExtFunc(2 * _, dsmapFunction) 
    output = mapDS1.reduceExtFunc(_ + _, dsreduceFunction)
    val ms11 = (System.nanoTime - now11) / 1000000
    println("No Cache DS Elapsed time: %d ms".format(ms11))
     println("DS Output is " + output)

    println(" ==== ")

    val now111 = System.nanoTime
    val mapDS11 = data111.mapExtFunc(2 * _, dsmapFunction) 
    output = mapDS11.reduceExtFunc(_ + _, dsreduceFunction)
    val ms111 = (System.nanoTime - now111) / 1000000
    println("Datapoint cached ; DS Elapsed time: %d ms".format(ms111))
     println("DS Output is " + output)

    val rangeData = rd.cacheGpu()
    rangeData.reduceExtFunc((x1, x2) => x2 , dsloadFunction)
    println(">>>  ==== ")
	
    val mapDS114 = rangeData.mapExtFunc(2 * _, dsmapFunction).cacheGpu().mapExtFunc(2 * _, dsmapFunction)
    timeit("ALL GPU", {
    println("count is " + mapDS114.count())
    mapDS114.collect().take(10).foreach(println)
    })

    val rangeData1 = rd.cacheGpu(true)
    rangeData1.reduceExtFunc((x1, x2) => x2 , dsloadFunction)
    println(" ==== ")
    val mapDS115 = rangeData1.mapExtFunc(2 * _, dsmapFunction).cacheGpu(true).mapExtFunc(2 * _, dsmapFunction)
    timeit("ALL GPU2", {
    println("ALL GPU Caching count :: " + mapDS115.count())
    println("ALL GPU Caching collect :: ")
    mapDS115.collect().take(10).foreach(println)
    })

    val now3 = System.nanoTime
    data.map(2 * _).reduce(_ + _)
    val ms3 = (System.nanoTime - now3) / 1000000
    println("CPU Elapsed time: %d ms".format(ms3))
   }

}


