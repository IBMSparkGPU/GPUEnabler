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
object SparkDSLRmod {
  val rand = new Random(42)

  case class DataPoint(x: Array[Double], y: Double)

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
      val y = if (i % 2 == 0) 0 else 1
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
    val spark = SparkSession.builder().master(masterURL).appName("SparkDSLR").getOrCreate()
    import spark.implicits._

    val numSlices = if (args.length > 1) args(1).toInt else 1
    val N = if (args.length > 2) args(2).toInt else 10000  // Number of data points
    val D = if (args.length > 3) args(3).toInt else 10   // Numer of dimensions
    val ITERATIONS = if (args.length > 4) args(4).toInt else 5

    val R = 0.7  // Scaling factor
    val ptxURL = "/LogisticRegression.ptx"

    val mapFunction = spark.sparkContext.broadcast(
      DSCUDAFunction(
        "map",
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
        "reducegrad",
        Array("value"),
        Array("value"),
        ptxURL,
        Some((size: Long) => 1),
        Some(gpuParams), outputSize=Some(1)))

    val pointsCached = spark.range(1, N+1, 1, numSlices).map(i => generateData(i, N, D, R)).cache()
    val pointsColumnCached = pointsCached.cacheGpu(true)

    // load data points into GPU
    pointsColumnCached.loadGpu()
    println("Data Generation Done !!")

    // Initialize w to a random value
    // var wCPU = Array.fill(D){2 * rand.nextDouble - 1}
    var wCPU = Array.fill[Double](D)(0.0)
    var wGPU = Array.tabulate(D)(i => wCPU(i))
    val wbc1 = spark.sparkContext.broadcast(wGPU)

    println("============ GPU =======================")
    print("Initial Weights :: ")
    wGPU.take(3).foreach(y => print(y + ", "))
    println(" ... ")

    val now = System.nanoTime
    for (i <- 1 to ITERATIONS) {
      val now1 = System.nanoTime
      val wGPUbcast = spark.sparkContext.broadcast(wGPU)
 
      val mapDS = pointsColumnCached.mapExtFunc((p: DataPoint) =>
        dmulvs(p.x,  (1 / (1 + exp(-p.y * (ddotvv(wGPU, p.x)))) - 1) * p.y),
        mapFunction.value, 
        Array(wGPUbcast.value, D), 
        outputArraySizes = Array(D)
      ).cacheGpu(true)

      val gradient = mapDS.reduceExtFunc((x: Array[Double], y: Array[Double]) => daddvv(x, y),
        reduceFunction.value, 
        Array(D), 
        outputArraySizes = Array(D))

      mapDS.unCacheGpu()

      wGPU = dsubvv(wGPU, gradient)

      val ms1 = (System.nanoTime - now1) / 1000000
      println(s"Iteration $i Done in $ms1 ms")
    }
    val ms = (System.nanoTime - now) / 1000000
    println("GPU Elapsed time: %d ms".format(ms))

    pointsColumnCached.unCacheGpu()

    wGPU.take(5).foreach(y => print(y + ", "))
    print(" .... ")
    wGPU.takeRight(5).foreach(y => print(y + ", "))
    println()	

    println("============ CPU =======================")
    val cnow = System.nanoTime
    for (i <- 1 to ITERATIONS) {
      val cnow1 = System.nanoTime

      val gradient = pointsCached.map { p =>
        dmulvs(p.x,  (1 / (1 + exp(-1 * ddotvv(wCPU, p.x)))) - p.y)
      }.reduce((x: Array[Double], y: Array[Double]) => daddvv(x, y))


      wCPU = dsubvv(wCPU, gradient)
      val cms1 = (System.nanoTime - cnow1) / 1000000
      println(s"Iteration $i Done in $cms1 ms")
    }

    val cms = (System.nanoTime - cnow) / 1000000
    println("CPU Elapsed time: %d ms".format(cms))

    wCPU.take(5).foreach(y => print(y + ", "))
    print(" .... ")
    wCPU.takeRight(5).foreach(y => print(y + ", "))
    println()

  }
}

