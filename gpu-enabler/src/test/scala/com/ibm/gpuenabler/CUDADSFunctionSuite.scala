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
case class MatrixData(M: Long, N: Long)

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

      val dimensions = (size: Long, stage: Int) => (((size + 32 * 8 - 1) / (32 * 8)).toInt, 32, 1, 1, 1, 1);
      val gpuParams = gpuParameters(dimensions)
      // we only use size/8 GPU threads and run block on a single warp
      val function = DSCUDAFunction(
        "blockXOR",
        Array("value"),
        Array("value"),
        ptxURL,
        None,
        Some(gpuParams))
      
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
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val function = DSCUDAFunction(
        "sum",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize = Some(1))
      val n = 30000

      try {
	val input: Dataset[lang.Integer] = spark.range(n+1).map(_.toInt)
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

  test("Run map on datasets - single partition", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val n = 10
      try {
        val output = spark.range(1, n+1, 1, 1)
          .mapExtFunc(_ * 2, mapFunction)
          .collect()
        
        assert(output.sameElements((1 to n).map(_ * 2)))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run reduce on datasets - single partition", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "sum",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 10
      try {
        val input:Dataset[lang.Integer] = spark.range(1, n+1, 1, 1).map(_.toInt)
        val output  = input.reduceExtFunc(_ + _, reduceFunction)
        assert(output == n * (n + 1) / 2)
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }


  test("Run map + reduce on datasets - single partition", GPUTest) {
    val spark = SparkSession.builder().master("local[1]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "suml",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 10
      try {
        val output = spark.range(1, n+1, 1, 1)
          .mapExtFunc(_ * 2, mapFunction)
          .reduceExtFunc(_ + _, reduceFunction)
        
        assert(output == n * (n + 1))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map on datasets with 100,000 elements - multiple partition", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val n = 100000
      try {
        val output = spark.range(1, n+1, 1, 16)
          .mapExtFunc(2 * _, mapFunction)
          .collect()
        assert(output.sameElements((1 to n).map(_ * 2)))
      } finally { 
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + reduce on datasets - multiple partitions", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "suml",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 100
      try {
        val output = spark.range(1, n+1, 1, 16)
          .mapExtFunc(2 * _, mapFunction)
          .reduceExtFunc(_ + _, reduceFunction)
        assert(output == n * (n + 1))
      } finally { 
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

 test("Run map + reduce on datasets with 100,000,000 elements - multiple partitions", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "suml",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n: Long = 100000000L
      try {
        val data = spark.range(1, n+1, 1, 16).cache()//.cacheGpu(true)
        data.count()
        val mapDS = data.mapExtFunc(2 * _, mapFunction).cacheGpu()
        val output: Long = mapDS.reduceExtFunc(_ + _, reduceFunction)
        mapDS.unCacheGpu()
        assert(output == n * (n + 1))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + map + reduce on datasets - multiple partitions", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "suml",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 100
      try {
        val output = spark.range(1, n+1, 1, 16)
          .mapExtFunc(_ * 2, mapFunction)
          .mapExtFunc(_ * 2, mapFunction)
          .reduceExtFunc(_ + _, reduceFunction)
        assert(output == 2 * n * (n + 1))
      } finally { 
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + map + map + collect on datasets", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val n = 100
      try {
        val output = spark.range(1, n+1, 1, 16)
          .mapExtFunc(_ * 2, mapFunction)
          .mapExtFunc(_ * 2, mapFunction)
          .mapExtFunc(_ * 2, mapFunction)
          .collect
        assert(output.sameElements((1 to n).map(_ * 8)))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }


  test("Run map + map + map + reduce on datasets - multiple partitions", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "suml",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 100
      try {
        val output = spark.range(1, n+1, 1, 16)
          .mapExtFunc(_ * 2, mapFunction)
          .mapExtFunc(_ * 2, mapFunction)
          .mapExtFunc(_ * 2, mapFunction)
          .reduceExtFunc(_ + _, reduceFunction)
        assert(output == 4 * n * (n + 1))
      } finally { 
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }
  
  test("Run map on dataset with a single primitive array column", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "intArrayIdentity",
        Array("value"),
        Array("value"),
        ptxURL)
      val n = 16
      val dataset = List(Array.range(0, n), Array.range(-(n-1), 1))
      try {
        val output = dataset.toDS()
          .mapExtFunc((x: Array[Int]) => x, mapFunction, Array(n), outputArraySizes = Array(n))
          .collect()
        assert(output(0).sameElements(0 to n-1))
        assert(output(1).sameElements(-(n-1) to 0))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map with free variables on dataset with a single primitive array column", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      def iaddvv(x: Array[Int], y: Array[Int]) : Array[Int] =
        Array.tabulate(x.length)(i => x(i) + y(i))

      val mapFunction = DSCUDAFunction(
        "intArrayAdd",
        Array("value"),
        Array("value"),
        ptxURL)
      val n = 16
      val v = Array.fill(n)(1)
      val dataset = List(Array.range(0, n), Array.range(-(n-1), 1))
      try {
        val output = dataset.toDS()
          .mapExtFunc((x: Array[Int]) => iaddvv(x, v),
            mapFunction, Array(v, n), outputArraySizes = Array(n))
          .collect()
        assert(output(0).sameElements(1 to n))
        assert(output(1).sameElements(-(n-2) to 1))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run reduce on dataset with a single primitive array column", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      def iaddvv(x: Array[Int], y: Array[Int]) : Array[Int] =
        Array.tabulate(x.length)(i => x(i) + y(i))

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "intArraySum",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 8
      val dataset = List(Array.range(0, n), Array.range(2*n, 3*n))
      try {
        val output = dataset.toDS()
          .reduceExtFunc((x: Array[Int], y: Array[Int]) => iaddvv(x, y),
          reduceFunction, Array(n), outputArraySizes = Array(n))
        assert(output.sameElements((n to 2*n-1).map(_ * 2)))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map & reduce on a single primitive array in a structure", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      def daddvv(x: Array[Double], y: Array[Double]) : Array[Double] = {
        Array.tabulate(x.length)(i => x(i) + y(i))
      }

      val mapFunction = spark.sparkContext.broadcast(
        DSCUDAFunction(
          "DataPointMap",
          Array("x", "y"),
          Array("value"),
          ptxURL))
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = spark.sparkContext.broadcast(
        DSCUDAFunction(
          "DataPointReduce",
          Array("value"),
          Array("value"),
          ptxURL,
          Some((size: Long) => 2),
          Some(gpuParams), outputSize=Some(1)))
      val n = 5
      val w = Array.fill(n)(2.0)
      val dataset = List(DataPoint(Array( 1.0, 2.0, 3.0, 4.0, 5.0), -1),
        DataPoint(Array( -5.0, -4.0, -3.0, -2.0, -1.0), 1))
      try {
        val input = dataset.toDS().cache()
        val output = input.mapExtFunc((p: DataPoint) => daddvv(p.x, w),
          mapFunction.value, Array(w, n), outputArraySizes = Array(n))
          .reduceExtFunc((x: Array[Double], y: Array[Double]) => daddvv(x, y),
          reduceFunction.value, Array(n), outputArraySizes = Array(n))

        assert(output.sameElements((0 to 4).map((x: Int) => x * 2.0)))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run logistic regression", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
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

      val mapFunction = spark.sparkContext.broadcast(
        DSCUDAFunction(
          "mapAll",
          Array("x", "y"),
          Array("value"),
          ptxURL))
      val threads = 1024
      val blocks = min((N + threads- 1) / threads, 1024)
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (blocks, threads, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = spark.sparkContext.broadcast(
        DSCUDAFunction(
          "blockReduce",
          Array("value"),
          Array("value"),
          ptxURL,
          Some((size: Long) => 1),
          Some(gpuParams), outputSize=Some(1)))

      def generateData: Array[DataPoint] = {
        def generatePoint(i: Int): DataPoint = {
          val y = if (i % 2 == 0) -1 else 1
          val x = Array.fill(D){rand.nextGaussian + y * R}
          DataPoint(x, y)
        }
        Array.tabulate(N)(generatePoint)
      }

      val pointsCached = spark.sparkContext.parallelize(generateData, numSlices).cache()
      val pointsColumnCached = pointsCached.toDS().cache().cacheGpu()

      // Initialize w to a random value
      var wCPU = Array.fill(D){2 * rand.nextDouble - 1}
      var wGPU = Array.tabulate(D)(i => wCPU(i))

      try {
        for (i <- 1 to ITERATIONS) {
          val wGPUbcast = spark.sparkContext.broadcast(wGPU)
          val gradient = pointsColumnCached.mapExtFunc((p: DataPoint) =>
            dmulvs(p.x,  (1 / (1 + exp(-p.y * (ddotvv(wGPU, p.x)))) - 1) * p.y),
            mapFunction.value, Array(wGPUbcast.value, D), outputArraySizes = Array(D)
          ).reduceExtFunc((x: Array[Double], y: Array[Double]) => daddvv(x, y),
            reduceFunction.value, Array(D), outputArraySizes = Array(D))
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
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map with Self on Dataset ", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager!= null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2_self",
        Seq("value"),
        Seq(),
        ptxURL)

      val n = 10
      try {
        val output = spark.range(1, n+1, 1, 1)
          .mapExtFunc(_ * 2, mapFunction)
          .collect()

        assert(output.sameElements((1 to n).map(_ * 2)))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("CUDA GPU Cache Testcase", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager!= null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Seq("value"),
        Seq("value"),
        ptxURL)

      val n = 10
      val baseDS = spark.range(1, n+1, 1, 1)
      var r1 = Array[Long](1)
      var r2 = Array[Long](1)

      try {
        for( i <- 1 to 2) {
          // With cache it should copy memory from cpu to GPU everytime.
          val n1 = baseDS.mapExtFunc(_ * 2, mapFunction).cacheGpu()
          r1 = n1.mapExtFunc(_ * 2, mapFunction).collect()
          r2 = baseDS.mapExtFunc(_ * 2, mapFunction).collect()
          assert(r1.sameElements(r2.map(_ * 2)))

          // With cache it should copy from CPU to GPU only one time.
          baseDS.cacheGpu()
          r1 = baseDS.mapExtFunc(_ * 2, mapFunction).collect()
          r2 = baseDS.mapExtFunc(_ * 2, mapFunction).collect()
          assert(r1.sameElements(r2))

          // UncacheGPU should clear the GPU cache.
          n1.unCacheGpu()
          baseDS.unCacheGpu().unCacheGpu()
          r1 = n1.collect()
          r2 = baseDS.mapExtFunc(_ * 2, mapFunction).collect()
          assert(r1.sameElements(r2))

	  baseDS.cache()
        }
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("CUDA GPU loadGpu Testcase", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager!= null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Seq("value"),
        Seq("value"),
        ptxURL)

      val n = 10
      val baseDS = spark.range(1, n+1, 1, 1)
      var r1 = Array[Long](1)
      var r2 = Array[Long](1)

      try {
        for( i <- 1 to 2) {
          // With cache it should copy memory from cpu to GPU everytime.
          val n1 = baseDS.mapExtFunc(_ * 2, mapFunction).cacheGpu()

          // With cache it should copy from CPU to GPU only one time.
          baseDS.loadGpu()
          r1 = baseDS.mapExtFunc(_ * 2, mapFunction).collect()
          r2 = baseDS.mapExtFunc(_ * 2, mapFunction).collect()
          assert(r1.sameElements(r2))

          // UncacheGPU should clear the GPU cache.
          n1.unCacheGpu()
          baseDS.unCacheGpu().unCacheGpu()
          r1 = n1.collect()
          r2 = baseDS.mapExtFunc(_ * 2, mapFunction).collect()
          assert(r1.sameElements(r2))

          baseDS.cache()
        }
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

test("Run map + map + map + reduce on datasets - Cached multiple partitions", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "suml",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 100
      try {
        val output = spark.range(1, n+1, 1, 16)
          .mapExtFunc(_ * 2, mapFunction).cacheGpu()
          .mapExtFunc(_ * 2, mapFunction).cacheGpu()
          .mapExtFunc(_ * 2, mapFunction).cacheGpu()
          .reduceExtFunc(_ + _, reduceFunction)
        assert(output == 4 * n * (n + 1))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run map + map + map + reduce on dataset - Cached multiple partitions - GPU Only", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import spark.implicits._
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val mapFunction = DSCUDAFunction(
        "multiplyBy2",
        Array("value"),
        Array("value"),
        ptxURL)

      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (64, 256, 1, 1, 1, 1)
        case 1 => (1, 1, 1, 1, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val reduceFunction = DSCUDAFunction(
        "suml",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 2),
        Some(gpuParams), outputSize=Some(1))

      val n = 100
      try {
        val output = spark.range(1, n+1, 1, 16)
          .mapExtFunc(_ * 2, mapFunction).cacheGpu(true)
          .mapExtFunc(_ * 2, mapFunction).cacheGpu(true)
          .mapExtFunc(_ * 2, mapFunction).cacheGpu(true)
          .reduceExtFunc(_ + _, reduceFunction)
        assert(output == 4 * n * (n + 1))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }

  test("Run matix multiplication", GPUTest) {
    val spark = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    val manager =  GPUSparkEnv.get.cudaManager
    if (manager != null) {
      val dimensions = (size: Long, stage: Int) => stage match {
        case 0 => (1, 3, 1, 3, 1, 1)
      }
      val gpuParams = gpuParameters(dimensions)
      val MRFunction = DSCUDAFunction(
        "MNKernel",
        Array("M","N"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 1),
        Some(gpuParams))

      try {
        import spark.implicits._
        val data = spark.range(1, 10, 1, 1).map(x => MatrixData(x, x)).cache()
        data.count()
        val output = data.mapExtFunc((x: MatrixData) => x.M, MRFunction, Array(3)).collect()
        val expected_values : Array[Long] = Array(30,36,42,66,81,96,102,126,150)
        assert(output.sameElements(expected_values))
      } finally {
        spark.stop
      }
    } else {
      info("No CUDA devices, so skipping the test.")
    }
  }
}


