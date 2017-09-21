package com.ibm.gpuenabler
 
import java.util.Random
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.ibm.gpuenabler.CUDADSImplicits._
import org.apache.spark.ml.linalg.Vectors
import scala.collection.mutable
import scala.math._
 
object GpuKMeans {
  case class DataPointKMeans(features: Array[Double])
  case class DataPointKMeansMod(features: Array[Double], norm: Double )
  case class ClusterIndexes(features: Array[Double], index: Int)
  case class Results(s0: Array[Int], s1: Array[Double], s2: Array[Double])
 
  def timeit[T](msg: String, code: => T): T ={
    val now1 = System.nanoTime
    val rc = code
    val ms1 = (System.nanoTime - now1) / 1000000
    println("%s Elapsed time: %d ms".format(msg, ms1))
    rc
  }
 
  def maxPoints(d: Int, k: Int, part: Int, nGpu: Int): Long = {
    val perCluster: Long = 4*450 + (8*450 + 1) *d * 2 // s0 + s1 + s2 
    val clusterBytes: Long = (perCluster * k * part)  
    val means: Long = 8 * k * ( 1 + d)
 
    val perPoint = 8 * d + 2 * 4 + 8 
    val available: Long = (10L * 1024  * 1024 * 1024 * nGpu) - clusterBytes - means // 10GB
    val maxPoints = available / perPoint
    if (maxPoints <= 0) throw new IllegalArgumentException(s"too big: k * dimensions for the partition count: ${k} * ${d} * ${part} ")
    maxPoints
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of KMeans and is given as an example!
        |Please use org.apache.spark.ml.clustering.KMeans for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]):Unit = {

    showWarning() 
 
    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val k: Int = if (args.length > 1) args(1).toInt else 32
    val N: Long = if (args.length > 2) args(2).toLong else 100000
    val d: Int= if (args.length > 3) args(3).toInt else 32
    val numSlices = if (args.length > 4) args(4).toInt else 1
    val iters: Int = if (args.length > 5) args(5).toInt else 5
    val nGpu: Int = if (args.length > 6) args(6).toInt else 1
    val cpugpu: Int = if (args.length > 7) args(7).toInt else 3
 
    if ( (N / numSlices) * 8L * d > Integer.MAX_VALUE) {
	println(s"Increase the partitions to stay within cuMemAllocHost limits")
        return
    }

    val maxPt = maxPoints(d, k, numSlices, nGpu)
    if ((cpugpu & 1) != 0 && N > maxPt) {
        println(s"N(${N}) is too high for the given d(${d}) & k(${k}) ; MAX ${maxPt}")
        return
    }
 
    println(s"KMeans (${k}) Algorithm on N: ${N} datasets; Dimension: ${d}; slices: ${numSlices} for ${iters} iterations")
    val spark = SparkSession.builder().master(masterURL).appName("SparkDSKMeans").getOrCreate()
    import spark.implicits._
 
    Logger.getRootLogger().setLevel(Level.ERROR)
 
    val data: Dataset[DataPointKMeans] = getDataSet(spark, N, d, numSlices)
    val dataMod: Dataset[DataPointKMeansMod] = getDataSetMod(spark, N, d, numSlices).as[DataPointKMeansMod]
    println("Data Generation Done")
 
    if ((cpugpu & 1) != 0) {
      println(" ======= GPU ===========")
      val (centers, cost) = runGpu(dataMod, d, k, iters, N)
      println(s"Cost: ${cost}")
    }
 
    if ((cpugpu & 2) != 0) {
      println(" ======= CPU ===========")
      val (ccenters, ccost) = run(dataMod, d, k, iters)
      // printCenters("Cluster centers:", ccenters)
      println(s"Cost: ${ccost}")
    }
  }
 
 def runGpu(data1: Dataset[DataPointKMeansMod], d: Int, k: Int,
          maxIterations: Int, N: Long): (Array[DataPointKMeansMod], Double) = {
 
    import data1.sparkSession.implicits._
    val sc = data1.sparkSession.sparkContext
 
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
 
    val centroidFnMod = sc.broadcast(DSCUDAFunction(
        "getClusterCentroidsMod",
        Array("features", "norm"),
        Array("index"), 
        ptxURL))
 
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
 
    val interFn = sc.broadcast(DSCUDAFunction(
        "calculateIntermediates",
        Array("features", "index"),
        Array("s0", "s1", "s2"),
        ptxURL,
        Some((size: Long) => 1),
        Some(gpuParams1), outputSize=Some(450)) )
 
    val dimensions2 = (size: Long, stage: Int) => stage match {
      case 0 => (1, modD, 1, modK, 1, 1)
    }
 
    val gpuParams2 = gpuParameters(dimensions2)
 
    val sumFn = sc.broadcast(DSCUDAFunction(
        "calculateFinal",
        Array("s0", "s1", "s2"),
        Array("s0", "s1", "s2"),
        ptxURL,
        Some((size: Long) => 1),
        Some(gpuParams2), outputSize=Some(1)) )
 
    val means: Array[DataPointKMeansMod] = data1.rdd.takeSample(true, k, 42)

    var oldMeans = means

    def func1(p: DataPointKMeans): ClusterIndexes = {
      ClusterIndexes(p.features, 0)
    }

    def func1Mod(point: DataPointKMeansMod): ClusterIndexes = {
      var bestCluster = 0
      var bestDistance = Double.PositiveInfinity

      var c = 0
      oldMeans.foreach { center =>
        var lowerBoundOfSqDist = center.norm - point.norm
        lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
        if (lowerBoundOfSqDist < bestDistance) {
          val dist = squaredDistance(point.features, center.features)

          if (bestDistance > dist) {
            bestCluster = c
            bestDistance = dist
          }
        }
        c += 1
      }

      ClusterIndexes(point.features, bestCluster)
    }

    def func2(c: ClusterIndexes): Results = {
      val d = oldMeans(0).features.size
      val k = oldMeans.length

      val s0 = Array.fill[Int](k)(0)
      val s1 = Array.fill[Double](d * k)(0)
      val s2 = Array.fill[Double](d * k)(0)

      s0(c.index) += 1

      for (dim <- 0 until d) {
        var coord = c.features(dim)

        s1(c.index * d + dim) += coord
        s2(c.index * d + dim) += coord * coord
      }

      Results(s0, s1, s2)
    }

    def func3(r1: Results, r2: Results): Results = {
      Results(addArr(r1.s0, r2.s0),
        addArr(r1.s1, r2.s1), addArr(r1.s2, r2.s2))
    }

    val data = data1.cacheGpu(true)
    timeit("Data loaded in GPU ", {
      data.loadGpu()
    })
 
    // Warm-Up
    val oldCentroids = oldMeans.flatMap(p => p.features)
    val oldCentroids_norm = oldMeans.map(p => p.norm)
 
    // this gets distributed
    val centroidIndex = data.mapExtFunc(func1Mod,
      centroidFnMod.value,
      Array(oldCentroids, oldCentroids_norm, k, d), outputArraySizes = Array(d)
    ).cacheGpu(true) 
 
    val interValues = centroidIndex
       .mapExtFunc(func2, interFn.value, Array(k, d),
           outputArraySizes = Array(k, k*d, k*d)).cacheGpu(true)
 
    var result = interValues
        .reduceExtFunc(func3, sumFn.value, Array(k, d),
           outputArraySizes = Array(k, k*d, k*d)) 
 
    centroidIndex.unCacheGpu
    interValues.unCacheGpu
 
    timeit("GPU :: ", {
     // while (changed && iteration < maxIterations) {
     while (iteration < maxIterations) {
      iteration += 1
      val oldCentroids = oldMeans.flatMap(p => p.features)
      val oldCentroids_norm = oldMeans.map(p => p.norm) 
 
      // this gets distributed
      val centroidIndex = data.mapExtFunc(func1Mod,
        centroidFnMod.value,
        Array(oldCentroids, oldCentroids_norm, k, d), outputArraySizes = Array(d)
      ).cacheGpu(true) 
 
      val interValues = centroidIndex
         .mapExtFunc(func2, interFn.value, Array(k, d),
             outputArraySizes = Array(k, k*d, k*d)).cacheGpu(true) 
 
      result = timeit(s"Iteration $iteration", { interValues
          .reduceExtFunc(func3, sumFn.value, Array(k, d),
             outputArraySizes = Array(k, k*d, k*d))  })
 
      centroidIndex.unCacheGpu
      interValues.unCacheGpu 
 
      val newMeans = getCentersMod(k, d, result.s0, result.s1) 

      val maxDelta = oldMeans.zip(newMeans)
        .map(squaredDistanceMod)
        .max 
        cost = getCost(k, d, result.s0, result.s1, result.s2)
      changed = maxDelta > epsilon

      oldMeans = newMeans
      //println(s"Cost @ iteration ${iteration} is ${cost}")
     }
    })

    data.unCacheGpu
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
 
  def trainMod(means: Array[DataPointKMeansMod], pointItr: Iterator[DataPointKMeansMod]):
      Iterator[Tuple3[Array[Int], Array[Double], Array[Double]]] = {
    val d = means(0).features.size
    val k = means.length
 
    val s0 = new Array[Int](k)
    val s1 = new Array[Double](d * k)
    val s2 = new Array[Double](d * k)
 
    pointItr.foreach(point => {
      var bestCluster = 0
      var bestDistance = Double.PositiveInfinity
 
      // for (c <- 0 until k) {
      var c = 0
      means.foreach { center => 
        var lowerBoundOfSqDist = center.norm - point.norm
        lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
        if (lowerBoundOfSqDist < bestDistance) {
          val dist = squaredDistance(point.features, center.features)
 
          if (bestDistance > dist) {
            bestCluster = c
            bestDistance = dist
          }
        }
        c += 1
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
 
  def getCentersMod(k: Int, d: Int, s0: Array[Int],
                 s1: Array[Double]): Array[DataPointKMeansMod] = {
    val dpt = Array.tabulate(k)(i => DataPointKMeans(Array.tabulate(d)(j => s1(i * d + j) / s0(i).max(1)) ))
    dpt.map(x => DataPointKMeansMod(x.features, Vectors.norm(Vectors.dense(x.features), 2.0)))
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
 
  def getCostMod(k: Int, d: Int, s0: Array[Int], s1: Array[Double]) = {
    var cost: Double = 0
 
    for (i <- 0 until d * k) {
      val mean = i / d
      val center = s1(i) / s0(mean).max(1)
 
      // TODO simplify this
      cost += center * (center * s0(mean) - 2 * s1(i)) + (s1(i) * s1(i))
    }
 
    cost
  }
 
  def run(data: Dataset[DataPointKMeansMod], d: Int, k: Int,
          maxIterations: Int): (Array[DataPointKMeansMod], Double) = {
 
    import data.sparkSession.implicits._
    val means: Array[DataPointKMeansMod] = data.rdd.takeSample(true, k, 42)
 
    val epsilon = 0.5
    var changed = true
    var iteration = 0
 
    var cost = 0.0
    var oldMeans = means
 
    timeit("CPU :: ", {
     // while (changed && iteration < maxIterations) {
     while (iteration < maxIterations) {
      iteration += 1
 
      // this gets distributed
      val result = timeit(s"Iteration $iteration", { 
                     data.mapPartitions(pointItr => trainMod(oldMeans, pointItr))
                       .reduce((x,y) =>
                         (addArr(x._1, y._1), addArr(x._2, y._2), addArr(x._3, y._3))) })
 
      val newMeans: Array[DataPointKMeansMod] = getCentersMod(k, d, result._1, result._2)
 
      val maxDelta = oldMeans.zip(newMeans)
        .map(squaredDistanceMod)
        .max
 
      cost = getCost(k, d, result._1, result._2, result._3)
 
      changed = maxDelta > epsilon
      oldMeans = newMeans
    //  println(s"Cost @ iteration ${iteration} is ${cost}")
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
 
  def generateDataMod(seed: Long, N: Long, D: Int, R: Double): DataPointKMeansMod = {
    val r = new Random(seed)
    def generatePoint(i: Long): DataPointKMeansMod = {
      val x = Array.fill(D){r.nextGaussian + i * R}
      DataPointKMeansMod(x, Vectors.norm(Vectors.dense(x), 2.0))
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
 
  private def getDataSetMod(spark: SparkSession, N: Long, d: Int, slices: Int): Dataset[DataPointKMeansMod] = {
    import spark.implicits._
    val R = 0.7  // Scaling factor
    val pointsCached = spark.range(1, N+1, 1, slices).map(i => generateDataMod(i, N, d, R)).cache
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
 
  def squaredDistanceMod(v1: DataPointKMeansMod, v2: DataPointKMeansMod): Double = {
    squaredDistance(v1.features, v2.features)
  }
 
  def squaredDistanceMod(p: (DataPointKMeansMod, DataPointKMeansMod)): Double = {
    squaredDistanceMod(p._1, p._2)
  }
 
  def squaredDistance(v1: DataPointKMeans, v2: DataPointKMeans): Double = {
    squaredDistance(v1.features, v2.features)
  }
  def squaredDistance(p: (DataPointKMeans, DataPointKMeans)): Double = {
    squaredDistance(p._1, p._2)
  }
}
