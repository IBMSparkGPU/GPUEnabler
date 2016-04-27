# GPU Enabler for Spark

This package brings GPU related capabilities to Spark Framework.
The following capabilities are provided by this package,
* load & initialize a user provided GPU kernel on executors 
  which has Nvidia GPU cards attached to it. 
* convert the data from partitions to a columnar format so that
  it can be easily fed into the GPU kernel.
* provide support for caching inside GPU for optimized performance.


## Requirements

This package is compatible with Spark 1.5+ and scala 2.11


| Spark Version |  Scala Version | Compatible version of Spark GPU |
| ------------- |-----------------|----------------------|
| `1.5+`        | `2.11`          |`1.0.0`               |

## Linking

You can link against this library (for Spark 1.5+) in your program at the following coordinates:

Using SBT:

```
libraryDependencies += "com.ibm" %% "gpu-enabler_2.11" % "1.0.0"
```

Using Maven:

```xml
<dependency>
    <groupId>com.ibm</groupId>
    <artifactId>gpu-enabler_2.11</artifactId>
    <version>1.0.0</version>
</dependency>
```

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

```
$ bin/spark-shell --packages com.ibm:gpu-enabler_2.11:1.0.0
```

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.


## Support for GPU Enabler package

* Support x86_64 and ppc64le
* Support OpenJDK and IBM JDK
* Support NVIDIA GPU with CUDA (we confirmed with CUDA 7.0)
* Support CUDA 7.0 and 7.5 (should work with CUDA 6.0 and 6.5)
* Support scalar variables in primitive scalar types and primitive array in RDD

## Examples

The recommended way to load and use GPU kernel is by using the following APIs, which are available in Scala.

The package comes with a set of examples. They can be tried out as follows,
`./bin/run-example GpuEnablerExample`

The Nvidia kernel used in these sample programs is available for download
[here](https://github.com/IBMSparkGPU/GPUEnabler/blob/master/examples/src/main/resources/GpuEnablerExamples.ptx).
The source for this kernel can be found [here](https://github.com/IBMSparkGPU/GPUEnabler/blob/master/examples/src/main/resources/GpuEnablerExamples.cu).


### Scala API

```scala
// import needed for the Spark GPU method to be added
import org.apache.spark.cuda._
import org.apache.spark.cuda.CUDARDDImplicits._

// Load a kernel function from the GPU kernel binary 
val ptxURL = SparkGPULR.getClass.getResource("/GpuEnablerExamples.ptx")

val mapFunction = new CUDAFunction(
        "multiplyBy2",      // Native GPU function to multiple a given no. by 2 and return the result
        Array("this"),      // Input arguments 
        Array("this"),      // Output arguments 
        ptxURL)
        
val reduceFunction = new CUDAFunction(
        "sum",                  // Native GPU function to sum the input argument and return the result
        Array("this"),          // Input arguments 
        Array("this"),          // Output arguments
        ptxURL)
        
// 1. "convert" the row based formating to columnar format
// 2. Apply a transformation ( multiple all the values of the RDD by 2)
// 3. Trigger a reduction action (sum up all the values and return the result)
val output = sc.parallelize(1 to n, 1)
        .convert(ColumnFormat)                      
        .mapExtFunc((x: Int) => 2 * x, mapFunction)  
        .reduceExtFunc((x: Int, y: Int) => x + y, reduceFunction)  
```


## Building From Source

### Pre-requisites

* NVidia GPU card with CUDA support of 7.0+.
* Install CUDA drivers & Runtime drivers for your platform from [here](https://developer.nvidia.com/cuda-downloads).

This library is built with [Maven](https://maven.apache.org/guides/index.html).  

To build a JAR file please follow these steps,
* `git clone https://github.com/IBMSparkGPU/GPUEnabler.git`
* `cd GPUEnabler`
* `./compile.sh`

Note:
* If mvn is not available in $PATH, export MVN_CMD="\<path_to_mvn_binary>" 
* If you want to use mvn from spark/build directory, add "--force" argument to `./compile.sh`


## Testing
To run the tests, you should run `mvn test`.

## On-going work
* Leverage existing schema awareness in DataFrame/DataSet
* Provide new DataFrame/DataSet operators to call CUDA Kernels
