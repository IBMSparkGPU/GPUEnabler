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
import org.apache.log4j.{Level, Logger}
import org.scalatest.FunSuite
import org.scalatest.Tag
import scala.language.implicitConversions
import scala.math._
import scala.reflect.ClassTag
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import CUDARDDImplicits._

object GPUTest extends Tag("com.ibm.gpuenabler.GPUTest")

case class Vector2DDouble(x: Double, y: Double)
case class VectorLength(len: Double)
case class PlusMinus(base: Double, deviation: Float)
case class FloatRange(a: Double, b: Float)
case class IntDataPoint(x: Array[Int], y: Int)
case class DataPoint(x: Array[Double], y: Double)

class CUDAFunctionSuite extends FunSuite {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)

  private val conf = new SparkConf(false).set("spark.driver.maxResultSize", "2g")

  test("Ensure CUDA kernel is serializable", GPUTest) {
    val sc = new SparkContext("local[*]", "test", conf)
    val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
    val function = new CUDAFunction(
      "identity",
      Array("this"),
      Array("this"),
      ptxURL)
    SparkEnv.get.closureSerializer.newInstance().serialize(function)
    sc.stop
  }

  test("Run count()", GPUTest) {
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null ) {
      val n = 32
      val output = sc.parallelize(1 to n, 4)
        .convert(com.ibm.gpuenabler.ColumnFormat)
        .count()
      assert(output == n)
    } else {
      info("No CUDA devices, so skipping the test.")
    }
    sc.stop
  }

  def getSchema[T: ClassTag]: ColumnPartitionSchema = {
    ColumnPartitionSchema.schemaFor[T]
  }

  test("Run identity CUDA kernel on a single primitive column", GPUTest) {

    val sc = new SparkContext("local[*]", "test", conf)
    try {
      val manager =  GPUSparkEnv.get.cudaManager
      if (manager != null) {
        val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
        val function = new CUDAFunction(
          "identity",
          Array("this"),
          Array("this"),
          ptxURL)
        val n = 1024

        val inputSchema = getSchema[Int]

        val input = new HybridIterator((1 to n).toArray, inputSchema,
          null, None, numentries = n, outputArraySizes = Seq(1))

        val output = function.compute[Int, Int](input, Seq(inputSchema, inputSchema),
          outputArraySizes = Seq(1))

        assert(output.asInstanceOf[HybridIterator[Int]].numElements == n)
        assert(inputSchema.isPrimitive)
        val outputItr = output
        assert(outputItr.toIndexedSeq.sameElements(1 to n))
        assert(!outputItr.hasNext)

        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } else {
        info("No CUDA devices, so skipping the test.")
      }
    } finally {
       sc.stop
    }

  }

  test("Run identity CUDA kernel on a single primitive array column", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val function = new CUDAFunction(
        "intArrayIdentity",
        Array("this"),
        Array("this"),
        ptxURL)
      val n = 16

      try {
        val inputSchema = getSchema[Array[Int]]

        val input = new HybridIterator(Array(Array.range(0, n),
          Array.range(-(n-1), 1)), inputSchema,
          null, None)

        val output = function.compute[Array[Int], Array[Int]](input,
          Seq(inputSchema, inputSchema), outputArraySizes = Seq(n))

        assert(output.asInstanceOf[HybridIterator[Int]].numElements == 2)
        assert(inputSchema.isPrimitive)
        val outputItr = output
        assert(outputItr.next.toIndexedSeq.sameElements(0 to n-1))
        assert(outputItr.next.toIndexedSeq.sameElements(-(n-1) to 0))
        assert(!outputItr.hasNext)
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Array[Int]]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run identity CUDA kernel on a single primitive array in a structure", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val function = new CUDAFunction(
        "IntDataPointIdentity",
        Array("this.x", "this.y"),
        Array("this.x", "this.y"),
        ptxURL)
      val n = 5
      val dataset = List(IntDataPoint(Array( 1, 2, 3, 4, 5), -10),
                         IntDataPoint(Array( -5, -4, -3, -2, -1), 10))

      try {
        val inputSchema = getSchema[IntDataPoint]

        val input = new HybridIterator(dataset.toArray, inputSchema,
          Array("this.x", "this.y"), None)

        val output = function.compute[IntDataPoint, IntDataPoint](input,
          Seq(inputSchema, inputSchema), outputArraySizes = Seq(n, 1))

        assert(output.asInstanceOf[HybridIterator[IntDataPoint]].numElements == 2)
        assert(!inputSchema.isPrimitive)
        val outputItr = output
        val next1 = outputItr.next
        assert(next1.x.toIndexedSeq.sameElements(1 to n))
        assert(next1.y == -10)
        val next2 = outputItr.next
        assert(next2.x.toIndexedSeq.sameElements(-n to -1))
        assert(next2.y ==  10)
        assert(!outputItr.hasNext)
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run add CUDA kernel with free variables on a single primitive array column", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val function = new CUDAFunction(
        "intArrayAdd",
        Array("this"),
        Array("this"),
        ptxURL)
      val n = 16
      val v = Array.fill(n)(1)

      try {
        val inputSchema = getSchema[Array[Int]]

        val input = new HybridIterator(Array(Array.range(0, n),
          Array.range(-(n-1), 1)), inputSchema,
          null, None)

        val output = function.compute[Array[Int], Array[Int]](input,
          Seq(inputSchema, inputSchema), outputArraySizes = Array(n),
          inputFreeVariables = Array(v))

        assert(output.asInstanceOf[HybridIterator[Array[Int]]].numElements  == 2)
        assert(inputSchema.isPrimitive)
        val outputItr = output
        assert(outputItr.next.toIndexedSeq.sameElements(1 to n))
        assert(outputItr.next.toIndexedSeq.sameElements(-(n-2) to 1))
        assert(!outputItr.hasNext)
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run vectorLength CUDA kernel on 2 col -> 1 col", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val function = new CUDAFunction(
        "vectorLength",
        Array("this.x", "this.y"),
        Array("this.len"),
        ptxURL)
      val n = 100
      val inputVals = (1 to n).flatMap { x =>
        (1 to n).map { y =>
          Vector2DDouble(x, y)
        }
      }

      try {
        val inputSchema = getSchema[Vector2DDouble]
        val outputSchema = getSchema[VectorLength]

        val input = new HybridIterator(inputVals.toArray, inputSchema,
          Array("this.x", "this.y"), None)

        val output = function.compute[VectorLength, Vector2DDouble](input,
          Seq(inputSchema, outputSchema), outputArraySizes = Array(1))
      
        assert(output.asInstanceOf[HybridIterator[VectorLength]].numElements == n * n)
        output.zip(inputVals.iterator).foreach { case (res, vect) =>
          assert(abs(res.len - sqrt(vect.x * vect.x + vect.y * vect.y)) < 1e-7)
        }
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run plusMinus CUDA kernel on 2 col -> 2 col", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val function = new CUDAFunction(
        "plusMinus",
        Array("this.base", "this.deviation"),
        Array("this.a", "this.b"),
        ptxURL)
      val n = 100
      val inputVals = (1 to n).flatMap { base =>
        (1 to n).map { deviation =>
          PlusMinus(base * 0.1, deviation * 0.01f)
        }
      }

      try {
        val inputSchema = getSchema[PlusMinus]
        val outputSchema = getSchema[FloatRange]

        val input = new HybridIterator(inputVals.toArray, inputSchema,
          Array("this.base", "this.deviation"), None)

        val output = function.compute[FloatRange, PlusMinus](input,
          Seq(inputSchema, outputSchema))

        assert(output.asInstanceOf[HybridIterator[FloatRange]].numElements == n * n)
        output.toIndexedSeq.zip(inputVals).foreach { case (range, plusMinus) =>
          assert(abs(range.b - range.a - 2 * plusMinus.deviation) < 1e-5f)
        }
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run applyLinearFunction CUDA kernel on 1 col + 2 const arg -> 1 col", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val function = new CUDAFunction(
        "applyLinearFunction",
        Array("this"),
        Array("this"),
        ptxURL,
        List(2: Short, 3: Short))
      val n = 1000

      try {
        val inputSchema = getSchema[Short]
        
        val input = new HybridIterator((1 to n).map(_.toShort).toArray, inputSchema,
          null, None)

        val output = function.compute[Short, Short](input,
          Seq(inputSchema, inputSchema))
        
        assert(output.asInstanceOf[HybridIterator[Short]].numElements == n)
        output.toIndexedSeq.zip((1 to n).map(x => (2 + 3 * x).toShort)).foreach {
          case (got, expected) => assert(got == expected)
        }
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }


  test("Run blockXOR CUDA kernel on 1 col + 1 const arg -> 1 col on custom dimensions", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      // we only use size/8 GPU threads and run block on a single warp
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val function = new CUDAFunction(
        "blockXOR",
        Array("this"),
        Array("this"),
        ptxURL,
        List(0x0102030411121314L),
        None,
        Some((size: Long, stage: Int) => (((size + 32 * 8 - 1) / (32 * 8)).toInt, 32)))
      val n = 10
      
      val inputVals = List.fill(n)(List(
          0x14, 0x13, 0x12, 0x11, 0x04, 0x03, 0x02, 0x01,
          0x34, 0x33, 0x32, 0x31, 0x00, 0x00, 0x00, 0x00
        ).map(_.toByte)).flatten

      try {
        val inputSchema = getSchema[Byte]

        val input = new HybridIterator(inputVals.toArray, inputSchema,
          null, None)

        val output = function.compute[Byte, Byte](input,
          Seq(inputSchema, inputSchema))
        
        assert(output.asInstanceOf[HybridIterator[Short]].numElements == 16 * n)
        val expectedOutputVals = List.fill(n)(List(
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x20, 0x20, 0x20, 0x20, 0x04, 0x03, 0x02, 0x01
          ).map(_.toByte)).flatten
        assert(output.toIndexedSeq.sameElements(expectedOutputVals))
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run sum CUDA kernel on 1 col -> 1 col in 2 stages", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256)
        case 1 => (1, 1)
      }
      val function = new CUDAFunction(
        "sum",
        Array("this"),
        Array("this"),
        ptxURL,
        Seq(),
        Some((size: Long) => 2),
        Some(dimensions))
      val n = 30000

      try {
        val inputSchema = getSchema[Int]

        val input = new HybridIterator((1 to n).toArray, inputSchema,
          null, None)

        val output = function.compute[Int, Int](input,
          Seq(inputSchema, inputSchema), outputSize = Some(1))
        
        assert(output.asInstanceOf[HybridIterator[Short]].numElements == 1)
        assert(output.next == n * (n + 1) / 2)
        input.freeGPUMemory
        output.asInstanceOf[HybridIterator[Int]].freeGPUMemory
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }


  test("Run map on rdds - single partition", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "multiplyBy2",
        Array("this"),
        Array("this"),
        ptxURL)

      val n = 10

      try {
        val output = sc.parallelize(1 to n, 1)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .collect()
        assert(output.sameElements((1 to n).map(_ * 2)))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map on rdds - multiple partition - test empty partition", GPUTest) {

    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "multiplyBy2",
        Array("this"),
        Array("this"),
        ptxURL)

      val n = 5

      try {
        val output = sc.parallelize(1 to n, 10)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .collect()
        assert(output.sameElements((1 to n).map(_ * 2)))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }


  test("Run reduce on rdds - single partition", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
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
      try {
        val output = sc.parallelize(1 to n, 1)
          .convert(com.ibm.gpuenabler.ColumnFormat)
          .reduceExtFunc((x: Int, y: Int) => x + y, reduceFunction)
        assert(output == n * (n + 1) / 2)
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + reduce on rdds - single partition", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
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
      try {
        val output = sc.parallelize(1 to n, 1)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .reduceExtFunc((x: Int, y: Int) => x + y, reduceFunction)
        assert(output == n * (n + 1))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map on rdds with 100,000 elements - multiple partition", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "multiplyBy2_l",
        Array("this"),
        Array("this"),
        ptxURL)

      val n: Long = 100000L
      try {
        val output = sc.parallelize(1L to n, 64)
          .mapExtFunc((x: Long) => 2 * x, mapFunction)
          .collect()
        assert(output.sameElements((1L to n).map(_ * 2)))
      } finally { 
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + reduce on rdds - multiple partitions", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
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

      val n = 100
      try {
        val output = sc.parallelize(1 to n, 16)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .reduceExtFunc((x: Int, y: Int) => x + y, reduceFunction)
        assert(output == n * (n + 1))
      } finally { 
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + reduce on rdds with 100,000,000 elements - multiple partitions", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "multiplyBy2_l",
        Array("this"),
        Array("this"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256)
        case 1 => (1, 1)
      }
      val reduceFunction = new CUDAFunction(
        "sum_l",
        Array("this"),
        Array("this"),
        ptxURL,
        Seq(),
        Some((size: Long) => 2),
        Some(dimensions))

      val n: Long = 100000000L
      try {
        val output: Long = sc.parallelize(1L to n, 64)
          .mapExtFunc((x: Long) => 2 * x, mapFunction)
          .reduceExtFunc((x: Long, y: Long) => x + y, reduceFunction)
        assert(output == n * (n + 1L))
      } finally { 
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + map + reduce on rdds - multiple partitions", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
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

      val n = 100
      try {
        val output = sc.parallelize(1 to n, 16)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .reduceExtFunc((x: Int, y: Int) => x + y, reduceFunction)
        assert(output == 2 * n * (n + 1))
      } finally { 
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + map + map + collect on rdds", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "multiplyBy2",
        Array("this"),
        Array("this"),
        ptxURL)

      val n = 100
      try {
        val output = sc.parallelize(1 to n, 16)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .collect
        assert(output.sameElements((1 to n).map(_ * 8)))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }


  test("Run map + map + map + reduce on rdds - multiple partitions", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
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

      val n = 100
      try {
        val output = sc.parallelize(1 to n, 16)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .mapExtFunc((x: Int) => 2 * x, mapFunction)
          .reduceExtFunc((x: Int, y: Int) => x + y, reduceFunction)
        assert(output == 4 * n * (n + 1))
      } finally { 
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }
  
  test("Run map on rdd with a single primitive array column", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "intArrayIdentity",
        Array("this"),
        Array("this"),
        ptxURL)
      val n = 16
      val dataset = List(Array.range(0, n), Array.range(-(n-1), 1))
      try {
        val output = sc.parallelize(dataset, 1)
          .mapExtFunc((x: Array[Int]) => x, mapFunction, outputArraySizes = Array(n))
          .collect()
        val outputItr = output.iterator
        assert(outputItr.next.toIndexedSeq.sameElements(0 to n-1))
        assert(outputItr.next.toIndexedSeq.sameElements(-(n-1) to 0))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map with free variables on rdd with a single primitive array column", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      def iaddvv(x: Array[Int], y: Array[Int]) : Array[Int] =
        Array.tabulate(x.length)(i => x(i) + y(i))

      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "intArrayAdd",
        Array("this"),
        Array("this"),
        ptxURL)
      val n = 16
      val v = Array.fill(n)(1)
      val dataset = List(Array.range(0, n), Array.range(-(n-1), 1))
      try {
        val output = sc.parallelize(dataset, 1)
          .mapExtFunc((x: Array[Int]) => iaddvv(x, v),
            mapFunction, outputArraySizes = Array(n),
            inputFreeVariables = Array(v))
          .collect()
        val outputItr = output.iterator
        assert(outputItr.next.toIndexedSeq.sameElements(1 to n))
        assert(outputItr.next.toIndexedSeq.sameElements(-(n-2) to 1))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run reduce on rdd with a single primitive array column", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      def iaddvv(x: Array[Int], y: Array[Int]) : Array[Int] =
        Array.tabulate(x.length)(i => x(i) + y(i))

      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256)
        case 1 => (1, 1)
      }
      val reduceFunction = new CUDAFunction(
        "intArraySum",
        Array("this"),
        Array("this"),
        ptxURL,
        Seq(),
        Some((size: Long) => 2),
        Some(dimensions))

      val n = 8
      val dataset = List(Array.range(0, n), Array.range(2*n, 3*n))
      try {
        val output = sc.parallelize(dataset, 1)
          .convert(com.ibm.gpuenabler.ColumnFormat)
          .reduceExtFunc((x: Array[Int], y: Array[Int]) => iaddvv(x, y),
          reduceFunction, outputArraySizes = Array(n))
        assert(output.toIndexedSeq.sameElements((n to 2*n-1).map(_ * 2)))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }


  test("Run map & reduce on a single primitive array in a structure", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      def daddvv(x: Array[Double], y: Array[Double]) : Array[Double] = {
        Array.tabulate(x.length)(i => x(i) + y(i))
      }

      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = sc.broadcast(
        new CUDAFunction(
          "DataPointMap",
          Array("this.x", "this.y"),
          Array("this"),
          ptxURL))
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256)
        case 1 => (1, 1)
      }
      val reduceFunction = sc.broadcast(
        new CUDAFunction(
          "DataPointReduce",
          Array("this"),
          Array("this"),
          ptxURL,
          Seq(),
          Some((size: Long) => 2),
          Some(dimensions)))
      val n = 5
      val w = Array.fill(n)(2.0)
      val dataset = List(DataPoint(Array( 1.0, 2.0, 3.0, 4.0, 5.0), -1),
        DataPoint(Array( -5.0, -4.0, -3.0, -2.0, -1.0), 1))
      try {
        val input = sc.parallelize(dataset, 2).cache()
        val output = input.mapExtFunc((p: DataPoint) => daddvv(p.x, w),
          mapFunction.value, outputArraySizes = Array(n),
          inputFreeVariables = Array(w))
          .reduceExtFunc((x: Array[Double], y: Array[Double]) => daddvv(x, y),
          reduceFunction.value, outputArraySizes = Array(n))

        assert(output.sameElements((0 to 4).map((x: Int) => x * 2.0)))
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run logistic regression", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      def dmulvs(x: Array[Double], c: Double) : Array[Double] =
        Array.tabulate(x.length)(i => x(i) * c)
      def daddvv(x: Array[Double], y: Array[Double]) : Array[Double] =
        Array.tabulate(x.length)(i => x(i) + y(i))
      def dsubvv(x: Array[Double], y: Array[Double]) : Array[Double] =
        Array.tabulate(x.length)(i => x(i) - y(i))
      def ddotvv(x: Array[Double], y: Array[Double]) : Double =
        (x zip y).foldLeft(0.0)((a, b) => a + (b._1 * b._2))

      val N = 8192  // Number of data points
      val D = 8   // Numer of dimensions
      val R = 0.7  // Scaling factor
      val ITERATIONS = 5
      val numSlices = 8
      val rand = new Random(42)

      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = sc.broadcast(
        new CUDAFunction(
          "mapAll",
          Array("this.x", "this.y"),
          Array("this"),
          ptxURL))
      val threads = 1024
      val blocks = min((N + threads- 1) / threads, 1024)
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (blocks, threads)
      }
      val reduceFunction = sc.broadcast(
        new CUDAFunction(
          "blockReduce",
          Array("this"),
          Array("this"),
          ptxURL,
          Seq(),
          Some((size: Long) => 1),
          Some(dimensions)))

      def generateData: Array[DataPoint] = {
        def generatePoint(i: Int): DataPoint = {
          val y = if (i % 2 == 0) -1 else 1
          val x = Array.fill(D){rand.nextGaussian + y * R}
          DataPoint(x, y)
        }
        Array.tabulate(N)(generatePoint)
      }

      val pointsCached = sc.parallelize(generateData, numSlices).cache()
      val pointsColumnCached = pointsCached.cache().cacheGpu()

      // Initialize w to a random value
      var wCPU = Array.fill(D){2 * rand.nextDouble - 1}
      var wGPU = Array.tabulate(D)(i => wCPU(i))

      try {
        for (i <- 1 to ITERATIONS) {
          val wGPUbcast = sc.broadcast(wGPU)
          val gradient = pointsColumnCached.mapExtFunc((p: DataPoint) =>
            dmulvs(p.x,  (1 / (1 + exp(-p.y * (ddotvv(wGPU, p.x)))) - 1) * p.y),
            mapFunction.value, outputArraySizes = Array(D),
            inputFreeVariables = Array(wGPUbcast.value)
          ).reduceExtFunc((x: Array[Double], y: Array[Double]) => daddvv(x, y),
            reduceFunction.value, outputArraySizes = Array(D))
          wGPU = dsubvv(wGPU, gradient)

        }
        pointsColumnCached.unCacheGpu().unpersist()

        for (i <- 1 to ITERATIONS) {
          val gradient = pointsCached.map { p =>
            dmulvs(p.x,  (1 / (1 + exp(-p.y * (ddotvv(wCPU, p.x)))) - 1) * p.y)
          }.reduce((x: Array[Double], y: Array[Double]) => daddvv(x, y))
          wCPU = dsubvv(wCPU, gradient)
        }
        pointsCached.unpersist()

        (0 until wGPU.length).map(i => {
          assert(abs(wGPU(i) - wCPU(i)) < 1e-7)
        })
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("CUDA GPU Cache Testcase", GPUTest) {
    
    val sc = new SparkContext("local[*]", "test", conf)
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager!= null) {
      val ptxURL = getClass.getResource("/testCUDAKernels.ptx")
      val mapFunction = new CUDAFunction(
        "multiplyBy2_self",
        Array("this"),
        Array("this"),
        ptxURL)

      val n = 10
      def mulby2(x: Int) = x * 2
      val baseRDD = sc.parallelize(1 to n, 1)
      var r1 = Array[Int](1)
      var r2 = Array[Int](1)

      try {
        for( i <- 1 to 2) {

          // Without cache it should copy memory from cpu to GPU everytime.
          r1 = baseRDD.mapExtFunc(mulby2, mapFunction).collect()
          r2 = baseRDD.mapExtFunc(mulby2, mapFunction).collect()
          assert(r2.sameElements(r2))

          // With cache it should copy from CPU to GPU only one time.
          baseRDD.cacheGpu()
          r1 = baseRDD.mapExtFunc(mulby2, mapFunction).collect()
          r2 = baseRDD.mapExtFunc(mulby2, mapFunction).collect()
          assert(r2.sameElements(r1.map(mulby2)))

          // UncacheGPU should clear the GPU cache.
          baseRDD.unCacheGpu().unCacheGpu()
          r1 = baseRDD.mapExtFunc((x: Int) => 2 * x, mapFunction).collect()
          r2 = baseRDD.mapExtFunc((x: Int) => 2 * x, mapFunction).collect()
          assert(r2.sameElements(r2))

          // Caching the RDD in CPU Memory shouldn't have any impact on caching the same in GPU Memory.
          baseRDD.cache()
        }
      } finally {
        sc.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }
}
