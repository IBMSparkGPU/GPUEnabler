/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.gpuenabler

import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.gpuenabler.Utils._
import org.apache.spark.sql.gpuenabler._

object GpuDSArrayMult {

   case class data(name : String, factor : Long, arr:Array[Long]);
   case class data1(name: String,result:Array[Long]);

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
    ds.map(x => x)
      .mapGPU[data1]("arrayMult",(1 to 10).map(_ * 3).toArray, (1 to 35).map(_.toLong).toArray)
      .select($"name", $"result")
      .show()
  }

}

