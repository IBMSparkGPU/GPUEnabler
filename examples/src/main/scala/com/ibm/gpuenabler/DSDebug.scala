package com.ibm.gpuenabler

import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.gpuenabler.Utils._
import org.apache.spark.sql.gpuenabler._


/**
 * Created by kmadhu on 15/3/16.
 */

case class data(name:String, cnt1:Long,cnt2:Long,sum:Long)
object DSDebug {

  case class data(name : String, cnt1 : Long, cnt2 : Long, sum : Long);
  case class data1(name: String,sum:Long);

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val ss = org.apache.spark.sql.SparkSession.builder.getOrCreate()
    import ss.implicits._

    // Utils.init(ss,"examples/src/main/resources/GPUFuncs.json")
    Utils.init(ss,"src/main/resources/GPUFuncs.json")

    //val ds = ss.read.json("examples/src/main/resources/data.json").as[data];
    val ds = ss.read.json("src/main/resources/data.json").as[data];

    ds.collect()(0).name

    ds.mapGPU[data1]("add").show()
    ds.mapGPU[data1]("mul").show()




    //println(d.namedetails.name.toList)

    //ds1.mapGPU("Nothing").show()

    // ds.queryExecution.debug.codegen()
/*
    ds.printSchema()
    ds.explain()
    ds.show()
*/


/*    val phyzicalPlan = ds2.queryExecution.executedPlan

    println(phyzicalPlan.executeCollect().toList)

    printf(ds2.queryExecution.analyzed.numberedTreeString);

    ds2.printSchema()
    ds2.explain()*/

    import org.apache.spark.sql.functions.udf


    // val input = readLine("prompt> ")
  }

}

