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

import org.apache.spark.{SparkContext, SparkConf}
import com.ibm.gpuenabler.CUDARDDImplicits._

object GpuEnablerExample {

  def main(args: Array[String]) = {
    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val sparkConf = new SparkConf().setAppName("GpuEnablerExample1").setMaster(masterURL)
    val sc = new SparkContext(sparkConf)

    val ptxURL = getClass.getResource("/GpuEnablerExamples.ptx")
    val mapFunction = new CUDAFunction(
      "multiplyBy2",
      Array("this"),
      Array("this"),
      ptxURL)

    val dimensions = (size: Long, stage: Int) => stage match {
      case 0 => (64, 256)
      case 1 => (1, 1)
    }
    val reduceFunction = new CUDAFunction(
      "sum",
      Array("this"),
      Array("this"),
      ptxURL,
      Seq(),
      Some((size: Long) => 2),
      Some(dimensions))

    val n = 10
    val output = sc.parallelize(1 to n, 1)
      .mapExtFunc((x: Int) => 2 * x, mapFunction)
      .reduceExtFunc((x: Int, y: Int) => x + y, reduceFunction)

    println("Sum of the list is " + output)
  }

}

