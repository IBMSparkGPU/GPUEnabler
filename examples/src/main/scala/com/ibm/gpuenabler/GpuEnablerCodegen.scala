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

// bin/spark-submit --jars gpu-enabler/target/gpu-enabler_2.10-1.0.0.jar
// --class com.ibm.gpuenabler.GpuEnablerCodegen examples/target/gpu-enabler-examples_2.10-1.0.0.jar

package com.ibm.gpuenabler

import org.apache.spark.{SparkContext, SparkConf}
import com.ibm.gpuenabler.CUDARDDImplicits._

object GpuEnablerCodegen {

  def main(args: Array[String]) = {
    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val sparkConf = new SparkConf().setAppName("GpuEnablerCodegen").setMaster(masterURL)
    sparkConf.set("spark.gpu.codegen", "true")

    val sc = new SparkContext(sparkConf)

    val n = 10
    val intOut = sc.parallelize(1 to n, 1)
      .mapGpu((x: Int) => 2 * x)
      .reduceGpu((x: Int, y: Int) => x + y)
    println("Int sum of the list is " + intOut)

    val doubleOut = sc.parallelize(1 to n, 1).map(x => x.toDouble)
      .mapGpu((x: Double) => 2.5D * x)
      .reduceGpu((x: Double, y: Double) => x + y)
    println("Double sum of the list is " + doubleOut)
  }

}

