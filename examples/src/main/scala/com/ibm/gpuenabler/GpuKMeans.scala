
package com.ibm.gpuenabler

import scala.language.implicitConversions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.ibm.gpuenabler.CUDADSImplicits._
import scala.collection.mutable

case class DataPointKMeans(features: Array[Double])
case class ClusterIndexes(features: Array[Double], index: Int)
case class Results(s0: Array[Int], s1: Array[Double], s2: Array[Double])

object GpuKMeans {
  def main(args: Array[String]) = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val k: Int = if (args.length > 1) args(1).toInt else 20

    val d: Int= if (args.length > 2) args(2).toInt else 2
    val iters: Int = if (args.length > 3) args(3).toInt else 50
    val inputPath = if (args.length > 4) args(4) else "src/main/resources/kmeans-samples.txt"

    val spark = SparkSession.builder().master(masterURL).appName("SparkDSKMeans").getOrCreate()
    import spark.implicits._

    Logger.getRootLogger().setLevel(Level.ERROR)

    val data: Dataset[DataPointKMeans] = getDataSet(spark, inputPath).cache
    val N = data.count()

    val ptxURL = "/SparkGPUKmeans.ptx"

    val centroidFn = spark.sparkContext.broadcast(
      DSCUDAFunction(
        "getClusterCentroids",
        Array("features"),
        Array("index"),
        ptxURL))

    val interFn = spark.sparkContext.broadcast(
      DSCUDAFunction(
        "calculateIntermediates",
        Array("features", "index"),
        Array("s0", "s1", "s2"),
        ptxURL))

    val sumFn = spark.sparkContext.broadcast(
      DSCUDAFunction(
        "calculateFinal",
        Array("s0", "s1", "s2"),
        Array("s0", "s1", "s2"),
        ptxURL))


    def func1(p: DataPointKMeans): ClusterIndexes = {
      ClusterIndexes(p.features, 0)
    }

    def func2(c: ClusterIndexes): Results = {
      Results(Array.empty, Array.empty, Array.empty)
    }

    def func3(r1: Results, r2: Results): Results = {
      Results(Helper1.addArr(r1.s0, r1.s0), 
        Helper1.addArr(r1.s1, r1.s1),Helper1.addArr(r1.s2, r1.s2))
    }

    val means: Array[DataPointKMeans] = data.rdd.takeSample(true, k, 42)
    val initialCentroids = means.flatMap(p => p.features)

    println("C length : " + initialCentroids.length)

    data.cacheGpu(true)
    data.loadGpu()

    val centroidIndex = data.mapExtFunc(func1,
      centroidFn.value,
      Array(initialCentroids, k, d), outputArraySizes = Array(d)
    ).cacheGpu()

    centroidIndex.collect().take(5).foreach(c => {
      c.features.foreach(println)
      println
      println(c.index)
      println
    })

    val interValues = centroidIndex.mapExtFunc(func2, interFn.value, Array(k, d)).cacheGpu()

    val results = interValues.reduceExtFunc(func3, sumFn.value, Array(k, d))
    results.s0.take(10).foreach(println)
    results.s1.take(10).foreach(println)
    results.s2.take(10).foreach(println)

//    println(" ======= CPU ===========")
//
//
//    val (centers, cost) = run(data, d, k, iters)
//
//    printCenters("Cluster centers:", centers)
//    println(s"Cost: ${cost}")
  }



  def train(means: Array[DataPointKMeans], pointItr: Iterator[DataPointKMeans]):
      Iterator[Tuple3[Array[Int], Array[Double], Array[Double]]] = {
    val d = means(0).features.size
    val k = means.length

    val s0 = new Array[Int](k)
    val s1 = new Array[Double](d * k)
    val s2 = new Array[Double](d * k)

    pointItr.foreach(point => {
      var bestCluster = 0
      var bestDistance = 0.0

      for (c <- 0 until k) {
        var dist = Helper1.squaredDistance(point.features, means(c).features)

        if (c == 0 || bestDistance > dist) {
          bestCluster = c
          bestDistance = dist
        }
      }

      s0(bestCluster) += 1

      for (dim <- 0 until d) {
        var coord = point.features(dim)

        s1(bestCluster * d + dim) += coord
        s2(bestCluster * d + dim) += coord * coord
      }

    })

    var flag = true

    new Iterator[Tuple3[Array[Int], Array[Double], Array[Double]]]{
      override def hasNext: Boolean = flag

      override def next: Tuple3[Array[Int], Array[Double], Array[Double]] = {
        flag = false
        (s0, s1, s2)
      }
    }
  }

  def getCenters(k: Int, d: Int, s0: Array[Int],
                 s1: Array[Double]): Array[DataPointKMeans] = {
    Array.tabulate(k)(i => DataPointKMeans(Array.tabulate(d)(j => s1(i * d + j) / s0(i).max(1))))
  }

  def getCost(k: Int, d: Int, s0: Array[Int], s1: Array[Double], s2: Array[Double]) = {
    var cost: Double = 0

    for (i <- 0 until d * k) {
      val mean = i / d
      val center = s1(i) / s0(mean).max(1)

      // TODO simplify this
      cost += center * (center * s0(mean) - 2 * s1(i)) + s2(i)
    }

    cost
  }

  def run(data: Dataset[DataPointKMeans], d: Int, k: Int,
          maxIterations: Int): (Array[DataPointKMeans], Double) = {

    import data.sparkSession.implicits._
    val means: Array[DataPointKMeans] = data.rdd.takeSample(true, k, 42)

    val epsilon = 0.5
    var changed = true
    var iteration = 0

    var cost = 0.0
    var oldMeans = means

    while (changed && iteration < maxIterations) {

      // this gets distributed

//      val result = data.map(point => train(oldMeans, point)).reduce((x,y) =>
//        (Helper1.addArr(x._1, y._1), Helper1.addArr(x._2, y._2), Helper1.addArr(x._3, y._3)))


      val result = data.mapPartitions(pointItr => train(oldMeans, pointItr)).reduce((x,y) =>
        (Helper1.addArr(x._1, y._1), Helper1.addArr(x._2, y._2), Helper1.addArr(x._3, y._3)))

      val newMeans: Array[DataPointKMeans] = getCenters(k, d, result._1, result._2)

      val maxDelta = oldMeans.zip(newMeans)
        .map(Helper1.squaredDistance)
        .max

      cost = getCost(k, d, result._1, result._2, result._3)

      changed = maxDelta > epsilon
      oldMeans = newMeans
      println(s"Cost @ iteration ${iteration} is ${cost}")
      iteration += 1

    }

    println("Finished in " + iteration + " iterations")

    (oldMeans, cost)
  }

  private def getDataSet(spark: SparkSession, path: String): Dataset[DataPointKMeans] = {
    import spark.implicits._

    val rawinputDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(path)

    val pointsCached = rawinputDF.map(x=> {

      val rowElem = x.getString(0).split(" ")
      val len = rowElem.length
      val buffer = new mutable.ListBuffer[Double]()
      (0 until len). foreach { idx =>
        if(!rowElem(idx).isEmpty)
          buffer += rowElem(idx).toDouble
      }

      DataPointKMeans(buffer.toArray)
    })

    pointsCached
  }

  private def printCenters(heading: String, centers: Array[DataPointKMeans]) = {
    println
    println(heading)
    centers.foreach(point => println("  " + point.features.mkString(" ") +";"))
  }

}

object Helper1{
  def squaredDistance(a: Seq[Double], b: Seq[Double]): Double = {
    require(a.length == b.length, "equal lengths")

    a.zip(b)
      .map { p => val diff = p._1 - p._2; diff * diff }
      .sum
  }

  def addInPlace(lhs: Array[Double], rhs: Array[Double]) = {
    require(lhs.length == rhs.length, "equal lengths")

    for (i <- 0 until lhs.length) {
      lhs(i) += rhs(i)
    }
  }

  def addInPlace(lhs: Array[Int], rhs: Array[Int]) = {
    require(lhs.length == rhs.length, "equal lengths")

    for (i <- 0 until lhs.length) {
      lhs(i) += rhs(i)
    }


  }

  def addArr(lhs: Array[Double], rhs: Array[Double]) = {
    require(lhs.length == rhs.length, "equal lengths")

    lhs.zip(rhs).map { case (x, y) => x + y }

  }

  def addArr(lhs: Array[Int], rhs: Array[Int]) = {
    require(lhs.length == rhs.length, "equal lengths")

    lhs.zip(rhs).map { case (x, y) => x + y }
  }

  def squaredDistance(v1: DataPointKMeans, v2: DataPointKMeans): Double = {
    squaredDistance(v1.features, v2.features)
  }
  def squaredDistance(p: (DataPointKMeans, DataPointKMeans)): Double = {
    squaredDistance(p._1, p._2)
  }
}

