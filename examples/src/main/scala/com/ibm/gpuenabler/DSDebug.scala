package com.ibm.gpuenabler

import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.gpuenabler.Utils._
import org.apache.spark.sql.gpuenabler._


/**
 * Created by kmadhu on 15/3/16.
 */

object DSDebug {

   case class data(name : String, factor : Long, arr:Array[Long]);
   case class data1(name: String,result:Array[Long]);
  //case class data(name : String, cnt1 : Long, cnt2 : Long, sum : Long,arr:Array[Long]);
  //case class data1(name: String,sum:Long);

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val ss = org.apache.spark.sql.SparkSession.builder.getOrCreate()
    import ss.implicits._

    println(args.toList)

    if(args.length > 0) {
      println("Setting debug Mode" + args(0))
      SparkEnv.get.conf.set("DebugMode", args(0))
    }


    Utils.init(ss,Utils.homeDir +"GPUEnabler/examples/src/main/resources/GPUFuncs.json")

    val ds = ss.read.json(Utils.homeDir + "GPUEnabler/examples/src/main/resources/data.json").as[data];

    ds.mapGPU[data1]("arrayTest").show()

  }

}

