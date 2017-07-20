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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.functions.lit
import com.ibm.gpuenabler.CUDADSImplicits._

object GpuDSArrayMult {

  case class jsonData(name : String, factor: Long, arr: Array[Long])
  case class inputData(name : String, factor: Long, arr: Array[Long], result: Array[Long])
  case class outputData(name: String, result: Array[Long])

  def main(args : Array[String]): Unit = {

    val ss = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    import ss.implicits._
    if(args.length > 0) {
      println("Setting debug Mode" + args(0))
      SparkEnv.get.conf.set("DebugMode", args(0))
    }

    val ptxURL = "/GpuEnablerExamples.ptx"

    // 1. Sample Map Operation - multiple every element in the array by 2
    val mulFunc = DSCUDAFunction("multiplyBy2", Seq("value"), Seq("value"), ptxURL)

    val N: Long = 100000

    val dataPts = ss.range(1, N+1, 1, 10).cache
    val results = dataPts.mapExtFunc(_ * 2, mulFunc).collect()
    println("Count is " + results.length)
    assert(results.length == N)

    val expResults = (1 to N.toInt).map(_ * 2)
    assert(results.sameElements(expResults))

    // 2. Sample Reduce Operation - Sum of all elements in the array
    val dimensions = (size: Long, stage: Int) => stage match {
      case 0 => (64, 256, 1, 1, 1, 1)
      case 1 => (1, 1, 1, 1, 1, 1)
    }

    val gpuParams = gpuParameters(dimensions)

    val sumFunc = DSCUDAFunction(
      "suml",
      Array("value"),
      Array("value"),
      ptxURL,
      Some((size: Long) => 2),
      Some(gpuParams), outputSize=Some(1))

    val results2 = dataPts
          .mapExtFunc(_ * 2, mulFunc)
          .reduceExtFunc(_ + _, sumFunc)

    println("Output is "+ results2)
    println("Expected is " + (N * (N + 1)))
    assert(results2 == N * (N + 1))

    // 3. Dataset - GPU Map - Dataset Operation.
    val ds = ss.read.json("src/main/resources/data.json").as[jsonData]

    val dds = ds.withColumn("result", lit(null: Array[Double] )).as[inputData]
    
    val dsFunc = DSCUDAFunction("arrayTest", Seq("factor", "arr"), Seq("result"), ptxURL)

    val mapDS = dds.mapExtFunc(x => outputData(x.name, x.result),
      dsFunc,
      Array((1 to 10).map(_ * 3).toArray, (1 to 35).map(_.toLong).toArray),
      outputArraySizes = Array(3))

    mapDS.select($"name", $"result").show()

  }
}
