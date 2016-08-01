package org.apache.spark.sql.gpuenabler

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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, _}
import org.apache.spark.sql.types.StructType

/**
  * Interface for generated predicate
  */
abstract class JCUDAInterface {
  def hasNext() : Boolean
  def next() : InternalRow
  def init(itr : java.util.Iterator[InternalRow])
}

/**
  * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[InternalRow]].
  */
object JCUDACodeGen extends Logging {

  def generate(inputSchema: StructType): JCUDAInterface = {
    val ctx = newCodeGenContext()

    val codeBody =
      s"""
      import org.apache.spark.sql.catalyst.InternalRow;
      import org.apache.spark.sql.types.StructType;

      class myIterator extends JCUDAIter {

         private scala.collection.Iterator<InternalRow> inputIter;
         private StructType schema;

         public void initSchema(StructType s) {
           schema = s;
         }

         public void init(int Index, scala.collection.Iterator<InternalRow> inp) {
           inputIter = inp;
         }
         public boolean hasNext() {
           return inputIter.hasNext();
         }
         public InternalRow next() {
           InternalRow r = (InternalRow) inputIter.next();
           System.out.println(r.toSeq(schema));
           System.out.println(r);
           return r;
         }
      }

     public JCUDAIter generate(Object[] references) {
       return new myIterator();
     }  """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]
    return p;
  }

  def generateFromFile(inputSchema: StructType): JCUDAInterface = {
    val ctx = newCodeGenContext()

    //val c = scala.io.Source.fromFile("/Users/madhusudanan/spark-projects/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    val c = scala.io.Source.fromFile("/home/kmadhu/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    val codeBody = c.filter(x=> (!(x.contains("REMOVE")))).map(x => x+"\n").mkString

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]
    return p;
  }

}

