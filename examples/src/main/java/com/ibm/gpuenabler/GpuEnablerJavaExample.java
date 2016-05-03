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
import scala.collection.mutable.StringBuilder;
import scala.reflect.ClassTag;
import java.util.*;

public class GpuEnablerJavaExample {

    public static void main(String[] args) {
        String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
        String masterURL = "local[*]";

        if (args.length > 0)
          masterURL = args[0];

        SparkConf conf = new SparkConf().setAppName("GpuEnablerJavaExample").setMaster(masterURL);
        JavaSparkContext sc = new JavaSparkContext(conf);

        int n = 10;
        List<Integer> range = new ArrayList<Integer>(n);
        for (int i = 1; i <= n; i++) {
            range.add(i);
        }

        JavaRDD<Integer> inputData = sc.parallelize(range).cache();

        ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.TYPE);

        // JavaCUDARDD<Integer> ci = new JavaCUDARDD(inputData, tag);
        JavaCUDARDD<Integer> jCRDD = new JavaCUDARDD(inputData.rdd(), tag);

        GpuEnablerJavaExample gp = new GpuEnablerJavaExample();
        URL ptxURL = gp.getClass().getResource("/GpuEnablerExamples.ptx");

        Seq<String> inputseq = new ArraySeq<String>(1);
        StringBuilder sb = new StringBuilder(1, "this");
        inputseq.addString(sb);

        List<String> li = new ArrayList<String>(1);
        li.add("this");

        JavaCUDAFunction mapFunction = new JavaCUDAFunction(
                "multiplyBy2",
                li,
                li,
                ptxURL);

        JavaCUDAFunction reduceFunction = new JavaCUDAFunction(
                "sum1",
                li,
                li,
                ptxURL);

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

