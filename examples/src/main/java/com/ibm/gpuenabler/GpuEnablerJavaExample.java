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
package com.ibm.gpuenabler;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import java.net.URL;
import com.ibm.gpuenabler.*;
import org.apache.spark.api.java.function.Function;
import scala.collection.mutable.ArraySeq;
import scala.collection.Seq;
import scala.Some;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;
import scala.reflect.ClassTag;
import java.util.*;

public class GpuEnablerJavaExample {
    public static void main(String[] args) {
        String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
        String masterURL = "local[*]";

        if (args.length > 0)
          masterURL = args[0];

        SparkConf conf = new SparkConf().setAppName("GpuEnablerJavaExample");
        conf.setMaster(masterURL);
        conf.set("spark.driver.extraJavaOptions", "-Xmn8g -Xgcthreads8 -Xdump:system:none -Xdump:heap:none -Xtrace:none -Xnoloa -Xdisableexplicitgc");
        conf.set("spark.driver.memory", "10g");

        JavaSparkContext sc = new JavaSparkContext(conf);

        int n = 1000;
        List<Integer> range = new ArrayList<Integer>(n);
        for (int i = 1; i <= n; i++) {
            range.add(i);
        }

        JavaRDD<Integer> inputData = sc.parallelize(range, 10).cache();

        ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.TYPE);

        JavaCUDARDD<Integer> jCRDD = new JavaCUDARDD(inputData.rdd(), tag);

        GpuEnablerJavaExample gp = new GpuEnablerJavaExample();
        URL ptxURL = gp.getClass().getResource("/GpuEnablerExamples.ptx");

        JavaCUDAFunction mapFunction = new JavaCUDAFunction(
                "multiplyBy2",
                Arrays.asList("this"),
                Arrays.asList("this"),
                ptxURL);

        /**
          * Function to compute the GPU Grid Size & Block Size
          * for each Stage specified by the user program
          */ 
        Function2<Long, Integer, Tuple2<Integer, Integer>> dimensions = 
          new Function2<Long, Integer, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Long size, Integer stage) throws Exception {
                Tuple2 ret = new Tuple2<Integer, Integer>(1, 1) ;
                switch (stage) {
                    case 0: ret =  new Tuple2<Integer, Integer>(64, 256);
                        break;
                    case 1 : ret =  new Tuple2<Integer, Integer>(1, 1);
                        break;
                }
                return ret;
            }
        };


        /**
          * Function to compute the number of stages required to perform
          * the required task; It depends mainly on the total number of
          * elements to process. In this case, we return 2 to denote 2
          * stages are required
          */
        Function<Long, Integer> stageCount = new Function<Long, Integer>() {
            public Integer call(Long size) throws Exception {
                return 2;
            }
        };
        
        JavaCUDAFunction reduceFunction = new JavaCUDAFunction(
                "sum",
                Arrays.asList("this"),
                Arrays.asList("this"),
                ptxURL,
                new ArraySeq(0),
                new Some(stageCount),
                new Some(dimensions));

        Integer output = jCRDD.mapExtFunc((new Function<Integer, Integer>() {
            public Integer call(Integer x) { return (2 * x); }
        }), mapFunction, tag).cacheGpu().reduceExtFunc((new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        }), reduceFunction);

        System.out.println("Sum of the list is " + output);
        sc.stop();
    }
}

