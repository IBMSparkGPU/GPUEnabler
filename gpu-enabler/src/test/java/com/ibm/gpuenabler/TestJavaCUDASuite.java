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

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.api.java.JavaRDD;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Some;
import scala.Tuple2;
import scala.collection.mutable.ArraySeq;
import scala.collection.Seq;
import scala.collection.mutable.StringBuilder;
import scala.reflect.ClassTag;


public class TestJavaCUDASuite implements Serializable {

    private transient JavaSparkContext sc;

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "JavaAPISuite");
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void MultiPart() {
        // Run map on rdds with 100,000 elements - multiple partition 
        int n = 100000;
        List<Integer> range = new ArrayList<Integer>(n);
        for (int i = 1; i <= n; i++) {
            range.add(i);
        }

        JavaRDD<Integer> inputData = sc.parallelize(range, 64).cache();
        ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.TYPE);
        JavaCUDARDD<Integer> ci = new JavaCUDARDD(inputData.rdd(), tag);
        TestJavaCUDASuite gp = new TestJavaCUDASuite();
        URL ptxURL = gp.getClass().getResource("/testCUDAKernels.ptx");

        JavaCUDAFunction mapFunction = new JavaCUDAFunction(
                "multiplyBy2",
                Arrays.asList("this"),
                Arrays.asList("this"),
                ptxURL);

        Long output = ci.mapExtFunc((new Function<Integer, Integer>() {
            public Integer call(Integer x) {
                return (2 * x);
            }
        }), mapFunction, tag).count();

        Assert.assertEquals("100000", output.toString());
        Assert.assertTrue("Output matches 100000", output == 100000);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void MultiSum() {
        int n = 10;
        List<Integer> range = new ArrayList<Integer>(n);
        for (int i = 1; i <= n; i++) {
            range.add(i);
        }

        JavaRDD<Integer> inputData = sc.parallelize(range).cache();
        ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.TYPE);
        JavaCUDARDD<Integer> ci = new JavaCUDARDD(inputData.rdd(), tag);
        TestJavaCUDASuite gp = new TestJavaCUDASuite();
        URL ptxURL = gp.getClass().getResource("/testCUDAKernels.ptx");

        JavaCUDAFunction mapFunction = new JavaCUDAFunction(
                "multiplyBy2",
                Arrays.asList("this"),
                Arrays.asList("this"),
                ptxURL);

        Function2 dimensions = new Function2<Long, Integer, Tuple2<Integer, Integer>>() {
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

        Function stageCount = new Function<Long, Integer>() {
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

        Integer output = ci.mapExtFunc((new Function<Integer, Integer>() {
            public Integer call(Integer x) {
                return (2 * x);
            }
        }), mapFunction, tag).cacheGpu().reduceExtFunc((new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        }), reduceFunction);

        Assert.assertEquals("110", output.toString());
        Assert.assertTrue("Output matches 110", output == 110);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void MultiMultiSum() {
        int n = 10;
        List<Integer> range = new ArrayList<Integer>(n);
        for (int i = 1; i <= n; i++) {
            range.add(i);
        }

        JavaRDD<Integer> inputData = sc.parallelize(range).cache();
        ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.TYPE);
        JavaCUDARDD<Integer> ci = new JavaCUDARDD(inputData.rdd(), tag);
        TestJavaCUDASuite gp = new TestJavaCUDASuite();
        URL ptxURL = gp.getClass().getResource("/testCUDAKernels.ptx");

        JavaCUDAFunction mapFunction = new JavaCUDAFunction(
                "multiplyBy2",
                Arrays.asList("this"),
                Arrays.asList("this"),
                ptxURL);

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

        Integer output = ci.mapExtFunc((new Function<Integer, Integer>() {
            public Integer call(Integer x) {
                return (2 * x);
            }
        }), mapFunction, tag).mapExtFunc((new Function<Integer, Integer>() {
            public Integer call(Integer x) {
                return (2 * x);
            }
        }), mapFunction, tag).reduceExtFunc((new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        }), reduceFunction);

        System.out.print("Sum of the list is " + output);

        Assert.assertEquals("220", output.toString());
        Assert.assertTrue("Output matches 110", output == 220);
    }

}

