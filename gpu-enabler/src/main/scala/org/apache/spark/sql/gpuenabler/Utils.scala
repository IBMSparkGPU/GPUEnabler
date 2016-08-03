package org.apache.spark.sql.gpuenabler;


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression, GenericInternalRow, IsNotNull, NullIntolerant, PredicateHelper, SortOrder, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import scala.collection.mutable.HashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
  * Created by madhusudanan on 26/07/16.
  */

case class MAPGPUExec[U](cf: CudaFunc, child: SparkPlan,encoder : Encoder[U])
  extends UnaryExecNode {

  lazy val inputSchema = child.schema
  lazy val outputSchema = encoder.schema

  override def output: Seq[Attribute] = {
    //child.output.map { a => a}
    outputSchema.toAttributes
  }


  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val childRDD = child.execute();

    childRDD.mapPartitionsWithIndex { (index, iter) =>

      //val buffer = JCUDACodeGen.generateFromFile(child.schema)
      val buffer = JCUDACodeGen.generate(inputSchema,outputSchema,cf,10)
      // val buffer = JCUDACodeGen.generate(child.schema)
      // val buffer = new JCUDAJava().generateIt(child.schema)
      buffer.init(iter.asJava)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          buffer.hasNext
        }
        override def next: InternalRow = buffer.next
      }
    }
  }
}

case class MAPGPU[U:Encoder](func: CudaFunc, child: LogicalPlan,encoder : Encoder[U])
  extends UnaryNode {

  // Schema is same
  //override def output: Seq[Attribute] = child.output.filter(p=> p.name.contains("cnt"))
  override def output: Seq[Attribute] = encoder.schema.toAttributes

  //Max output rows for array length
  override def maxRows: Option[Long] = child.maxRows
}



object GPUOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case MAPGPU(cf, child,encoder) =>
      MAPGPUExec(cf, planLater(child),encoder) :: Nil
    case _ => {
      Nil
    }
  }
}

case class Args(val dtype:String, val name: String)
case class Func(val fname:String, val ptxPath: String)
case class CudaFunc(val func: Func, val inputArgs : Array[Args], val outputArgs : Array[Args])

object Utils {

  import org.apache.spark


  type _Column = org.apache.spark.sql.Column

  val cudaFunc : mutable.HashMap[String,CudaFunc] = new HashMap[String,CudaFunc]

  def init(ss : SparkSession, fname : String): Unit = {
    import ss.implicits._
    val c = ss.read.json(fname).as[CudaFunc]
    c.foreach(x=>cudaFunc += x.func.fname -> x)
  }

  implicit class tempClass[T: Encoder](ds: Dataset[T]) {


    def mapGPU[U:Encoder](inp: String): Dataset[U] =  {
      val cf = cudaFunc(inp)
      val encoder = implicitly[Encoder[U]]
      Dataset[U](ds.sparkSession, MAPGPU[U](cf, ds.logicalPlan,encoder))
    }

    ds.sparkSession.experimental.extraStrategies =
      (GPUOperators :: Nil)

/*    def changeType[U : Encoder](inp : String): Dataset[U] = {
      //val deserialized = CatalystSerde.deserialize[T](ds.logicalPlan)
      val mapped = MAPGPU(
        inp,
        ds.logicalPlan)
      CatalystSerde.serialize[U](mapped)
      Dataset[U](ds.sparkSession, MAPGPU("hi",ds.logicalPlan))
    }*/
  }

}
