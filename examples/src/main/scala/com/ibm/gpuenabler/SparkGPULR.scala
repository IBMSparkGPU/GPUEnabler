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
import scala.math._
import org.apache.spark._
import scala.language.implicitConversions
import com.ibm.gpuenabler.CUDARDDImplicits._

// scalastyle:off println
object SparkGPULR {
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

  def generateData(seed: Int, N: Int, D: Int, R: Double): DataPoint = {
    val r = new Random(seed)
    def generatePoint(i: Int): DataPoint = {
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
    val sparkConf = new SparkConf().setAppName("SparkGPULR").setMaster(masterURL)
    val sc = new SparkContext(sparkConf)
    val numSlices = if (args.length > 1) args(1).toInt else 2
    val N = if (args.length > 2) args(2).toInt else 10000  // Number of data points
    val D = if (args.length > 3) args(3).toInt else 10   // Numer of dimensions
    val ITERATIONS = if (args.length > 4) args(4).toInt else 5

    val R = 0.7  // Scaling factor

    val ptxURL = SparkGPULR.getClass.getResource("/SparkGPUExamples.ptx")
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

    val skelton = sc.parallelize((1 to N), numSlices)
    val points = skelton.map(i => generateData(i, N, D, R)).cache
    points.count()

    println("Data generation done")

    // Initialize w to a random value
    var wCPU = Array.fill(D){2 * rand.nextDouble - 1}
    var w = Array.tabulate(D)(i => wCPU(i))

    printf("numSlices=%d, N=%d, D=%d, ITERATIONS=%d\n", numSlices, N, D, ITERATIONS)
    points.cacheGpu()

    val now = System.nanoTime
    for (i <- 1 to ITERATIONS) {
      println("GPU iteration " + i)
      val wbc = sc.broadcast(w)
      val mapRdd = points.mapExtFunc((p: DataPoint) =>
        dmulvs(p.x, (1 / (1 + exp(-p.y * (ddotvv(wbc.value, p.x)))) - 1) * p.y),
        mapFunction.value, outputArraySizes = Array(D),
        inputFreeVariables = Array(wbc.value)
      ).cacheGpu
      val gradient = mapRdd.reduceExtFunc((x: Array[Double], y: Array[Double]) => daddvv(x, y),
        reduceFunction.value, outputArraySizes = Array(D))
      mapRdd.unCacheGpu()
      w = dsubvv(w, gradient)
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

    case class DataOut1(mr: Array[Double])

    def dmulvs1(x: Array[Double], c: Double) : DataOut1 =
      DataOut1(Array.tabulate(x.length)(i => x(i) * c))
    def daddvv1(x: DataOut1, y: DataOut1) : DataOut1 =
      DataOut1(Array.tabulate(x.mr.length)(i => x.mr(i) + y.mr(i)))
    def dsubvv1(x: Array[Double], y: DataOut1) : Array[Double] =
      Array.tabulate(x.length)(i => x(i) - y.mr(i))

    val now2 = System.nanoTime
    for (i <- 1 to ITERATIONS) {
      println("CPU iteration " + i)
      val wwCPU = sc.broadcast(wCPU)
      val gradient = points.map((p: DataPoint) =>
        dmulvs1(p.x,  (1 / (1 + exp(-p.y * ddotvv(wwCPU.value, p.x))) - 1) * p.y)).reduce((x: DataOut1, y: DataOut1) => daddvv1(x, y))

      wCPU = dsubvv1(wCPU, gradient)
    }
    val ms2 = (System.nanoTime - now2) / 1000000
    println("Elapsed time: %d ms".format(ms2))

    wCPU.take(5).foreach(y => print(y + ", "))
    print(" .... ")
    wCPU.takeRight(5).foreach(y => print(y + ", "))
    println()

    sc.stop()
  }
}
