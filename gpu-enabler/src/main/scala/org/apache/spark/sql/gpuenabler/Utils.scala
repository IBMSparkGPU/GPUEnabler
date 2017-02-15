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
package org.apache.spark.sql.gpuenabler

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution._
import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.collection.JavaConverters._
import scala.collection.mutable

case class MAPGPUExec[T, U](cf: CudaFunc, args : Array[AnyRef], 
        child: SparkPlan, 
        inputEncoder: Encoder[T], outputEncoder: Encoder[U], 
        outputObjAttr: Attribute)
  extends ObjectConsumerExec with ObjectProducerExec  {

  lazy val inputSchema = inputEncoder.schema
  lazy val outputSchema = outputEncoder.schema

  override def output: Seq[Attribute] = outputObjAttr :: Nil

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, 
       "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val inexprEnc = inputEncoder.asInstanceOf[ExpressionEncoder[T]]
    val outexprEnc = outputEncoder.asInstanceOf[ExpressionEncoder[U]]

    val childRDD = child.execute()

    childRDD.mapPartitionsWithIndex { (index, iter) =>
      val buffer = JCUDACodeGen.generate(inputSchema,outputSchema,cf,args)
      val list = new mutable.ListBuffer[InternalRow]
      iter.foreach(x => 
        list += inexprEnc.toRow(x.get(0, inputSchema).asInstanceOf[T]).copy())

      buffer.init(list.toIterator.asJava,args,list.size)
      
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          buffer.hasNext
        }
        override def next: InternalRow = 
          InternalRow(outexprEnc
            .resolveAndBind(outputEncoder.schema.toAttributes)
            .fromRow(buffer.next.copy()))
      }
    }
  }
}

object MAPGPU
{
  def apply[T: Encoder, U : Encoder]( 
      func: CudaFunc, args : Array[AnyRef], child: LogicalPlan) : LogicalPlan = {
    val deserialized = CatalystSerde.deserialize[T](child)
    val mapped = MAPGPU(
      func, args, 
      deserialized,
      implicitly[Encoder[T]],
      implicitly[Encoder[U]],
      CatalystSerde.generateObjAttr[U]
      )
    CatalystSerde.serialize[U](mapped)
  }
}

case class MAPGPU[T: Encoder, U : Encoder](func: CudaFunc, 
        args : Array[AnyRef], child: LogicalPlan, 
        inputEncoder: Encoder[T], outputEncoder: Encoder[U], 
        outputObjAttr: Attribute)
  extends ObjectConsumer with ObjectProducer {
   override def otherCopyArgs : Seq[AnyRef] = inputEncoder :: outputEncoder ::  Nil
}

object GPUOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case MAPGPU(cf, args, child,inputEncoder, outputEncoder, outputObjAttr) =>
      MAPGPUExec(cf, args, planLater(child),
        inputEncoder, outputEncoder, outputObjAttr) :: Nil
    case _ => {
      Nil
    }
  }
}

case class Args(length: String, name : String)
case class Func(fname:String, ptxPath: String)
case class CudaFunc(func: Func, inputArgs : Array[Args], outputArgs : Array[Args])

object Utils {

  type _Column = org.apache.spark.sql.Column

  val cudaFunc : mutable.HashMap[String,CudaFunc] = new HashMap[String,CudaFunc]

  def homeDir = System.getProperty("user.dir").split("GPUEnabler")(0)

  def init(ss : SparkSession, fname : String): Unit = {
    import ss.implicits._
    val c = ss.read.json(fname).as[CudaFunc]
    c.show()
    c.foreach(x=>cudaFunc += x.func.fname -> x)
  }

  implicit class tempClass[T: Encoder](ds: Dataset[T]) {

    def mapGPU[U:Encoder](inp: String, args: AnyRef*): Dataset[U] =  {
      val cf = cudaFunc(inp)
      val encoder = implicitly[Encoder[U]]

      Dataset[U](ds.sparkSession, MAPGPU[T, U](cf, args.toArray, ds.logicalPlan))
    }

    ds.sparkSession.experimental.extraStrategies =
      (GPUOperators :: Nil)
  }
}
