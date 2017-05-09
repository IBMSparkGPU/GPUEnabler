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
import java.lang
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
import org.apache.spark.sql.Dataset
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

      val input: Dataset[lang.Long] = spark.range(1, n+1, 1, 4)
      val output = input.mapExtFunc(_ * 1, cf).count()

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

  test("Run add CUDA kernel with free variables on a single primitive array column", GPUTest) {

    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      
      val function = DSCUDAFunction(
        "intArrayAdd",
        Array("value"),
        Array("value"),
        ptxURL)
      val n = 16
      val v = Array.fill(n)(1)

      try {
        val input = Seq(Array.range(0, n),
          Array.range(-(n-1), 1)).toDS()
        
        val output = input.mapExtFunc((x: Array[Int]) => x,
          function, Array(v, n),  outputArraySizes = Array(n)).collect()

        assert(output.length  == 2)
        assert(output(0).sameElements(1 to n))
        assert(output(1).sameElements(-(n-2) to 1))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run vectorLength CUDA kernel on 2 col -> 1 col", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val function = DSCUDAFunction(
        "vectorLength",
        Array("x", "y"),
        Array("len"),
        ptxURL)
      val n = 100
      val inputVals = (1 to n).flatMap { x =>
        (1 to n).map { y =>
          Vector2DDouble(x, y)
        }
      }

      try {
        val input = inputVals.toDS()
        val output = input.mapExtFunc((in: Vector2DDouble) => VectorLength(in.x * in.y),
          function, outputArraySizes = Array(1)).collect()
      
        assert(output.length == n * n)
        output.toIterator.zip(inputVals.iterator).foreach { case (res, vect) =>
          assert(abs(res.len - sqrt(vect.x * vect.x + vect.y * vect.y)) < 1e-7)
        }
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run plusMinus CUDA kernel on 2 col -> 2 col", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val function = DSCUDAFunction(
        "plusMinus",
        Array("base", "deviation"),
        Array("a", "b"),
        ptxURL)
      
      val n = 100
      val inputVals = (1 to n).flatMap { base =>
        (1 to n).map { deviation =>
          PlusMinus(base * 0.1, deviation * 0.01f)
        }
      }

      try {
        val input = inputVals.toDS()
        val output = input.mapExtFunc((in: PlusMinus) => FloatRange(in.base +in.deviation,
          (in.base - in.deviation).toFloat), function).collect()

        assert(output.length == n * n)
        output.zip(inputVals).foreach { case (range, plusMin) =>
          assert(abs(range.b - range.a - 2 * plusMin.deviation) < 1e-5f)
        }
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run applyLinearFunction CUDA kernel on 1 col + 2 const arg -> 1 col", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val function = DSCUDAFunction(
        "applyLinearFunction",
        Array("value"),
        Array("value"),
        ptxURL)
      val n: Short = 1000

      try {
        val input:Dataset[lang.Short] = spark.range(1, n+1, 1, 1).map(_.toShort)
        val output = input.mapExtFunc[lang.Short]((x: lang.Short) => x, function, Array(2: Short, 3: Short)).collect()
        
        assert(output.length == n)
        output.zip((1 to n).map(x => (2 + 3 * x).toShort)).foreach {
          case (got, expected) => assert(got == expected)
        }
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run blockXOR CUDA kernel on 1 col + 1 const arg -> 1 col on custom dimensions", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      // we only use size/8 GPU threads and run block on a single warp
      val function = DSCUDAFunction(
        "blockXOR",
        Array("value"),
        Array("value"),
        ptxURL,
        None,
        Some((size: Long, stage: Int) => (((size + 32 * 8 - 1) / (32 * 8)).toInt, 32)))
      
      val n = 10
      
      val inputVals = List.fill(n)(List(
          0x14, 0x13, 0x12, 0x11, 0x04, 0x03, 0x02, 0x01,
          0x34, 0x33, 0x32, 0x31, 0x00, 0x00, 0x00, 0x00
        ).map(_.toByte)).flatten.asInstanceOf[List[lang.Byte]]

      try {
        val input = inputVals.toDS()
        val output = input.mapExtFunc[lang.Byte]((x: lang.Byte) => x, function, Array(0x0102030411121314L)).collect()
        
        assert(output.length == 16 * n)
        val expectedOutputVals = List.fill(n)(List(
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x20, 0x20, 0x20, 0x20, 0x04, 0x03, 0x02, 0x01
          ).map(_.toByte)).flatten
        assert(output.sameElements(expectedOutputVals))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run sum CUDA kernel on 1 col -> 1 col in 2 stages", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256)
        case 1 => (1, 1)
      }
      val function = DSCUDAFunction(
        "sum",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(dimensions), outputSize = Some(1))
      val n = 30000

      try {
	val input: Dataset[lang.Integer] = spark.range(n+1).map(_.toInt)
        // val output = input.mapExtFunc[lang.Integer]((x: lang.Integer) => x, function).collect()
        val output = input.mapExtFunc(_ * 1, function).collect()
        
        assert(output.length == 1)
        assert(output(0) == n * (n + 1) / 2)
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

}


