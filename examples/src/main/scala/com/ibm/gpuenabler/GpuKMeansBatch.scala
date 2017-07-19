package com.ibm.gpuenabler

import java.util.Random
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.ibm.gpuenabler.CUDADSImplicits._
import scala.collection.mutable

object GpuKMeansBatch {
  case class DataPointKMeans(features: Array[Double])
  case class ClusterIndexes(features: Array[Double], index: Int)
  case class Results(s0: Array[Int], s1: Array[Double], s2: Array[Double])

  def timeit(msg: String, code: => Any): Any ={
    val now1 = System.nanoTime
    code
    val ms1 = (System.nanoTime - now1) / 1000000
    println("%s Elapsed time: %d ms".format(msg, ms1))
  }

  def maxPoints(d: Int, k: Int, part: Int, nGpu: Int): Long = {
    val perCluster: Long = 4*450 + (8*450 + 1) *d * 2 // s0 + s1 + s2
    val clusterBytes: Long = perCluster * k  * 2 * part // we allocate twice

    val perPoint = 8 * d + 2 * 4
    val available: Long = (10L * 1024  * 1024 * 1024 * nGpu) - clusterBytes // 10GB
    val maxPoints = available / perPoint
    if (maxPoints <= 0) throw new IllegalArgumentException(s"too big: k * dimensions for the partition count: ${k} * ${d} * ${part} ")
    maxPoints
  }

  def main(args: Array[String]):Unit = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val k: Int = if (args.length > 1) args(1).toInt else 32
    val N: Long = if (args.length > 2) args(2).toLong else 100000
    val d: Int= if (args.length > 3) args(3).toInt else 32
    val numSlices = if (args.length > 4) args(4).toInt else 1
    val iters: Int = if (args.length > 5) args(5).toInt else 5
    val nGpu: Int = if (args.length > 6) args(6).toInt else 1

    if (N > maxPoints(d, k, numSlices, nGpu)) {
      println(s"N(${N}) is too high for the given d(${d}) & k(${k}) ; MAX ${maxPoints(d, k, numSlices,nGpu) }")
      return
    }

    println(s"KMeans (${k}) Algorithm on N: ${N} datasets; Dimension: ${d}; slices: ${numSlices} for ${iters} iterations")
    val spark = SparkSession.builder().master(masterURL).appName("SparkDSKMeans").getOrCreate()
    spark.sparkContext.setCheckpointDir("/tmp")

    import spark.implicits._

    Logger.getRootLogger().setLevel(Level.ERROR)

    val data: Dataset[DataPointKMeans] = getDataSet(spark, N, d, numSlices)
    println("Data Generation Done")

    println(" ======= GPU ===========")

    val (centers, cost) = runGpu(data, d, k, iters)

    // printCenters("Cluster centers:", centers)
    println(s"Cost: ${cost}")
   
    println(" ======= CPU ===========")

    val (ccenters, ccost) = run(data, d, k, iters)

    // printCenters("Cluster centers:", ccenters)
    println(s"Cost: ${ccost}")
  }

  def runGpu(data: Dataset[DataPointKMeans], d: Int, k: Int,
             maxIterations: Int): (Array[DataPointKMeans], Double) = {

    import data.sparkSession.implicits._

    val epsilon = 0.5
    var changed = true
    var iteration = 0
    var cost = 0.0
    val ptxURL = "/SparkGPUKmeans.ptx"

    val centroidFn = DSCUDAFunction(
      "getClusterCentroids",
      Array("features"),
      Array("index"),
      ptxURL)

    var limit = 1024
    var modD = Math.min(d, limit)
    var modK = Math.min(k, limit)
    if (modD * modK > limit) {
      if (modD <= modK)
        modD = Math.max(limit/modK, 1)
      else
        modK = Math.max(limit/modD, 1)
    }

    println(s"Dimension are modD x modK : ${modD} x ${modK}")

    val dimensions1 = (size: Long, stage: Int) => stage match {
      case 0 => (450, modD, 1, modK, 1, 1)
    }

    val gpuParams1 = gpuParameters(dimensions1)

    val interFn = DSCUDAFunction(
      "calculateIntermediates",
      Array("features", "index"),
      Array("s0", "s1", "s2"),
      ptxURL,
      Some((size: Long) => 1),
      Some(gpuParams1), outputSize=Some(450))


    val dimensions2 = (size: Long, stage: Int) => stage match {
      case 0 => (1, modD, 1, modK, 1, 1)
    }

    val gpuParams2 = gpuParameters(dimensions2)

    val sumFn = DSCUDAFunction(
      "calculateFinal",
      Array("s0", "s1", "s2"),
      Array("s0", "s1", "s2"),
      ptxURL,
      Some((size: Long) => 1),
      Some(gpuParams2), outputSize=Some(1))


    def func1(p: DataPointKMeans): ClusterIndexes = {
      ClusterIndexes(p.features, 0)
    }

    def func2(c: ClusterIndexes): Results = {
      Results(Array.empty, Array.empty, Array.empty)
    }

    def func3(r1: Results, r2: Results): Results = {
      Results(addArr(r1.s0, r2.s0),
        addArr(r1.s1, r2.s1),addArr(r1.s2, r2.s2))
    }

    val means: Array[DataPointKMeans] = data.rdd.takeSample(true, k, 42)
    
    val dataSplits = data.randomSplit(Array(0.5,0.5), 42)

    // dataSplits.foreach(ds=> { timeit("Data loaded in GPU", { ds.cacheGpu(true);  ds.loadGpu(); }) } )

    var oldMeans = means
    var batch = 1
    
    timeit("GPU :: ", {
      while (iteration < maxIterations) {
        var s0 = new Array[Int](k)
        var s1 = new Array[Double](d * k)
        var s2 = new Array[Double](d * k)
        
        val oldCentroids = oldMeans.flatMap(p => p.features)

        dataSplits.foreach(datasplit => {
          println(s"Executing batch ${batch} for iteration $iteration ")
          batch += 1
          datasplit.cacheGpu(true)
          // timeit("Data loaded in GPU", { datasplit.loadGpu })

          // this gets distributed
          val centroidIndex = datasplit.mapExtFunc(func1,
            centroidFn,
            Array(oldCentroids, k, d), outputArraySizes = Array(d)
          ).cacheGpu(true)

          val interValues = centroidIndex
            .mapExtFunc(func2, interFn, Array(k, d),
              outputArraySizes = Array(k, k*d, k*d)).cacheGpu(true)

          val result = interValues
            .reduceExtFunc(func3, sumFn, Array(k, d),
              outputArraySizes = Array(k, k*d, k*d))

          centroidIndex.unCacheGpu
          interValues.unCacheGpu
          datasplit.unCacheGpu
          
          s0 = addArr(s0, result.s0)
          s1 = addArr(s1, result.s1)
          s2 = addArr(s2, result.s2)
        })

        val newMeans = getCenters(k, d, s0, s1)

        val maxDelta = oldMeans.zip(newMeans)
          .map(squaredDistance)
          .max

        cost = getCost(k, d, s0, s1, s2)

        changed = maxDelta > epsilon
        oldMeans = newMeans
        // println(s"Cost @ iteration ${iteration} is ${cost}")
        iteration += 1
      }
    })
    println("Finished in " + iteration + " iterations")
    (oldMeans, cost)
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
        var dist = squaredDistance(point.features, means(c).features)

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

    timeit("CPU :: ", {
      // while (changed && iteration < maxIterations) {
      while (iteration < maxIterations) {

        // this gets distributed
        val result = data.mapPartitions(pointItr => train(oldMeans, pointItr)).reduce((x,y) =>
          (addArr(x._1, y._1), addArr(x._2, y._2), addArr(x._3, y._3)))

        val newMeans: Array[DataPointKMeans] = getCenters(k, d, result._1, result._2)

        val maxDelta = oldMeans.zip(newMeans)
          .map(squaredDistance)
          .max

        cost = getCost(k, d, result._1, result._2, result._3)

        changed = maxDelta > epsilon
        oldMeans = newMeans
        //  println(s"Cost @ iteration ${iteration} is ${cost}")
        iteration += 1
      }
    })

    println("Finished in " + iteration + " iterations")
    (oldMeans, cost)
  }

  def generateData(seed: Long, N: Long, D: Int, R: Double): DataPointKMeans = {
    val r = new Random(seed)
    def generatePoint(i: Long): DataPointKMeans = {
      val x = Array.fill(D){r.nextGaussian + i * R}
      DataPointKMeans(x)
    }
    generatePoint(seed)
  }

  private def getDataSet(spark: SparkSession, N: Long, d: Int, slices: Int): Dataset[DataPointKMeans] = {
    import spark.implicits._
    val R = 0.7  // Scaling factor
    val pointsCached = spark.range(1, N+1, 1, slices).map(i => generateData(i, N, d, R)).cache
    pointsCached.count()
    pointsCached
  }

  private def printCenters(heading: String, centers: Array[DataPointKMeans]) = {
    println
    println(heading)
    centers.foreach(point => println("  " + point.features.mkString(" ") +";"))
  }

  def squaredDistance(a: Seq[Double], b: Seq[Double]): Double = {
    require(a.length == b.length, "equal lengths")

    a.zip(b)
      .map { p => val diff = p._1 - p._2; diff * diff }
      .sum
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
