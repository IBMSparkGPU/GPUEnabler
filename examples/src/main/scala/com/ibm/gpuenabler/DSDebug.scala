package com.ibm.gpuenabler

import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import com.ibm.gpuenabler.CUDADSImplicits._

object DSDebug {

   case class data(ele: Long, arr2: Array[Long], name : String, factor : Long, arr:Array[Long]);
   case class data1(ele: Long, name : String, arr2: Array[Long] , result:Array[Long]);

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val ss = org.apache.spark.sql.SparkSession.builder.getOrCreate()
    import ss.implicits._

    println(args.toList)
    // val ptxURL = DSDebug.getClass.getResource("/GpuEnablerExamples.ptx")
    val ptxURL = "/GpuEnablerExamples.ptx"

    val blocksize = 2
    val gridsize = 512
    val dimensions = (size: Long, stage: Int) => stage match {
      case 0 => (gridsize, blocksize)
      case 1 => (256, 4)
    }

    val dsFunc = DSCUDAFunction("arrayTestModStages", Seq("factor", "arr"), Seq("result"), ptxURL,
      Seq((1 to 35).map(_.toLong).toArray), Some((size: Long) => 2), Some(dimensions))
    val redFunc = DSCUDAFunction("arrayTestMod", Seq("ele", "result"), Seq("arr2"), ptxURL, outputSize = Some(1))

    if(args.length > 0) {
      println("Setting debug Mode" + args(0))
      SparkEnv.get.conf.set("DebugMode", args(0))
    }

    val ds = ss.read.json("src/main/resources/data.json").as[data];

    val gpuDS = ds.mapExtFunc(x=> data1(x.ele, x.name, x.arr2, x.arr.map(y => y * x.factor * 100)),
      dsFunc,
      Array((1 to 10).map(_ * 3).toArray, (1 to 35).map(_.toLong).toArray),
      Seq(3)).cacheGPU()

    val result = gpuDS.reduceExtFunc((d1: data1, d2: data1) => data1((d1.ele + d2.ele), d1.name, d1.arr2, d1.result), redFunc,
      Array((1 to 10).map(_ * 3).toArray, (1 to 35).map(_.toLong).toArray))

    println(s"RESULT ${result.ele}")

    gpuDS.uncacheGPU()
     
    gpuDS.map(x => x.result).show()
  }

}


