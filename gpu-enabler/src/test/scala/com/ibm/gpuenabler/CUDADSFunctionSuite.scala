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

import java.util.Random

import com.ibm.gpuenabler.CUDADSImplicits._
import org.apache.log4j.{Level, Logger}
import org.scalatest.FunSuite
import org.scalatest.Tag

import scala.language.implicitConversions
import scala.math._
import scala.reflect.ClassTag
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import com.ibm.gpuenabler.CUDADSImplicits._

object GPUTest extends Tag("com.ibm.gpuenabler.GPUTest")

case class Vector2DDouble(x: Double, y: Double)
case class VectorLength(len: Double)
case class PlusMinus(base: Double, deviation: Float)
case class FloatRange(a: Double, b: Float)
case class IntDataPoint(x: Array[Int], y: Int)
case class DataPoint(x: Array[Double], y: Double)

class CUDADSFunctionSuite extends FunSuite {

  val rootLogger = Logger.getRootLogger()
  val ptxURL = "/testDSCUDAKernels.ptx"
  rootLogger.setLevel(Level.OFF)

  private val conf = new SparkConf(false).set("spark.driver.maxResultSize", "2g")

  test("Ensure CUDA Function is serializable", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val cf = DSCUDAFunction("identity",
      Array("this"),
      Array("this"),
      ptxURL)
    
    SparkEnv.get.closureSerializer.newInstance().serialize(cf)
  }

  test("Run count()", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._

    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null ) {
      val n = 32

      val cf = DSCUDAFunction("identity",
        Array("value"),
        Array("value"),
        ptxURL)

      val output = spark.range(1, n+1, 1, 4).mapExtFunc(_ * 1, cf)
        .count()

      assert(output == n)
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run identity CUDA kernel on a single primitive array column", GPUTest) {

    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val function = DSCUDAFunction(
        "intArrayIdentity",
        Array("value"),
        Array("value"),
        ptxURL)
      val n = 16

      try {
        val input = Seq(Array.range(0, n),
          Array.range(-(n-1), 1)).toDS()

        val output = input.mapExtFunc((x: Array[Int]) => x, function, Array(n), 
		outputArraySizes = Array(n)).collect()

        assert(output.length == 2)
        assert(output(0).sameElements(0 to n-1))
        assert(output(1).sameElements(-(n-1) to 0))

      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run identity CUDA kernel on a single primitive array in a structure", GPUTest) {

    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val function = DSCUDAFunction(
        "IntDataPointIdentity",
        Array("x", "y"),
        Array("x", "y"),
        ptxURL)
      val n = 5
      val dataset = Seq(IntDataPoint(Array( 1, 2, 3, 4, 5), -10),
                         IntDataPoint(Array( -5, -4, -3, -2, -1), 10)).toDS

      try {
        val output = dataset.mapExtFunc((x: IntDataPoint) => x, function,
          Array(n), outputArraySizes = Array(n, 1)).collect()

        assert(output.length == 2)

        assert(output(0).x.sameElements(1 to n))
        assert(output(0).y ==  -10)
        assert(output(1).x.sameElements(-n to -1))
        assert(output(1).y ==  10)
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

}


