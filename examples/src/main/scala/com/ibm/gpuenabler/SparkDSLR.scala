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
import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions
import scala.math._

// scalastyle:off println
object SparkDSLR {
  val rand = new Random(42)

  case class DataPoint(x: Array[Double], y: Double)

  case class Results(result1: Array[Double], result2: Array[Double])

  def dmulvsMod(x: Array[Double], c: Double) : Results =
    Results(Array.tabulate(x.length)(i => x(i) * c), Array.tabulate(x.length)(i => x(i) * c))
  def daddvvMod(R1: Results, R2: Results) : Results = {
	println("R1.result2.length : " + R1.result1.length + " R2.result2.length " + R2.result1.length)
    Results(Array.empty, Array.tabulate[Double](R2.result2.length)(i => R1.result2(i) + R2.result2(i)))
   }


  def dmulvs(x: Array[Double], c: Double) : Array[Double] =
    Array.tabulate(x.length)(i => x(i) * c)
  def daddvv(x: Array[Double], y: Array[Double]) : Array[Double] =
    Array.tabulate(x.length)(i => x(i) + y(i))
  def dsubvv(x: Array[Double], y: Array[Double]) : Array[Double] =
    Array.tabulate(x.length)(i => x(i) - y(i))
  def ddotvv(x: Array[Double], y: Array[Double]) : Double =
    (x zip y).foldLeft(0.0)((a, b) => a + (b._1 * b._2))

  def generateData(seed: Long, N: Int, D: Int, R: Double): DataPoint = {
    val r = new Random(seed)
    def generatePoint(i: Long): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = Array.fill(D){r.nextGaussian + y * R}
      DataPoint(x, y)
    }
    generatePoint(seed)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    showWarning()

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    // val sparkConf = new SparkConf().setAppName("SparkGPULR").setMaster(masterURL)
    // val sc = new SparkContext(sparkConf)

    val spark = SparkSession.builder().master(masterURL).appName("SparkDSLR").getOrCreate()
    import spark.implicits._

    val numSlices = if (args.length > 1) args(1).toInt else 1
    val N = if (args.length > 2) args(2).toInt else 10000  // Number of data points
    val D = if (args.length > 3) args(3).toInt else 10   // Numer of dimensions
    val ITERATIONS = if (args.length > 4) args(4).toInt else 5

    val R = 0.7  // Scaling factor

    val threads = 1024
    val blocks = min((N + threads- 1) / threads, 1024)
    val dimensions = (size: Long, stage: Int) => stage match {
      case 0 => (blocks, threads)
    }

    //val ptxURL = SparkDSLR.getClass.getResource("/SparkGPUExamples.ptx")
    val ptxURL = "/SparkGPUExamples.ptx"
    val mapFunction = spark.sparkContext.broadcast(
        new DSCUDAFunction(
        "dsmapAll",
        Array("x", "y"),
        Array("result1"),
        ptxURL))
    
    val reduceFunction = spark.sparkContext.broadcast(
      new DSCUDAFunction(
        "dsblockReduce",
        Array("result1"),
        Array("result2"),
        ptxURL,
        outputSize=Some(1)))

    val skeleton = spark.range(1, N, 1, numSlices)
    val points = skeleton.map(i => generateData(i, N, D, R)).cache
    points.count()

    println("Data generation done")

    // Initialize w to a random value
    var wCPU = Array.fill(D){2 * rand.nextDouble - 1}
    var w = Array.tabulate(D)(i => wCPU(i))

    printf("numSlices=%d, N=%d, D=%d, ITERATIONS=%d\n", numSlices, N, D, ITERATIONS)
    points.cacheGpu()

    val now = System.nanoTime

/*
      val wbc = spark.sparkContext.broadcast(w)
      val mapDS = points.mapExtFunc((p: DataPoint) =>
        dmulvsMod(p.x, (1 / (1 + exp(-p.y * ddotvv(wbc.value, p.x))) - 1) * p.y),
        mapFunction.value, Array(wbc.value, D), outputArraySizes = Array(D)
      )
 	mapDS.select('result1, 'result2).show()
      val gradient = mapDS.reduceExtFunc((x: Results, y: Results) => daddvvMod(x, y),
        reduceFunction.value, Array(D), outputArraySizes = Array(D))
	
      gradient.result2.foreach(println)
*/

    for (i <- 1 to ITERATIONS) {
      println("GPU iteration " + i)
      val wbc = spark.sparkContext.broadcast(w)
      val mapDS = points.mapExtFunc((p: DataPoint) =>
        dmulvsMod(p.x, (1 / (1 + exp(-p.y * ddotvv(wbc.value, p.x))) - 1) * p.y),
        mapFunction.value, Array(wbc.value, D), outputArraySizes = Array(D)
      ).cacheGpu
      val gradient = mapDS.reduceExtFunc((x: Results, y: Results) => daddvvMod(x, y),
        reduceFunction.value, Array(D), outputArraySizes = Array(D))
      mapDS.unCacheGpu()
      w = dsubvv(w, gradient.result2)
    }

    val ms = (System.nanoTime - now) / 1000000
    println("Elapsed time: %d ms".format(ms))

    // pointsColumnCached.unCacheGpu()
    points.unCacheGpu()

    w.take(5).foreach(y => print(y + ", "))
    print(" .... ")
    w.takeRight(5).foreach(y => print(y + ", "))
    println()

    println("===================================")

  }
}

