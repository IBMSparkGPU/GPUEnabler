package org.apache.spark.sql.gpuenabler;


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression, GenericInternalRow, IsNotNull, NullIntolerant, PredicateHelper, SortOrder, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import scala.collection.JavaConverters._
/**
  * Created by madhusudanan on 26/07/16.
  */

case class MAPGPUExec(inp: String, child: SparkPlan)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = {
    child.output.map { a => a}
  }

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    child.printSchema()
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndex { (index, iter) =>

      val buffer = JCUDACodeGen.generateFromFile(child.schema)
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

case class MAPGPU(inp: String, child: LogicalPlan)
  extends UnaryNode {

  // Schema is same
  override def output: Seq[Attribute] = child.output

  //Max output rows for array length
  override def maxRows: Option[Long] = child.maxRows
}



object GPUOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case MAPGPU(inp, child) =>
      println("Mapping MAPGPU -> MAPGPUExec")
      MAPGPUExec(inp, planLater(child)) :: Nil
    case _ => {
      Nil
    }
  }
}

object Utils {

  type _Column = org.apache.spark.sql.Column

  implicit class tempClass[T: Encoder](ds: Dataset[T]) {
    def mapGPU(inp: String): Dataset[T] =  {
      Dataset(ds.sparkSession, MAPGPU(inp, ds.logicalPlan))
    }
    ds.sparkSession.experimental.extraStrategies =
      (GPUOperators :: Nil)
  }

  def getGenericInternalRow(internalRow: InternalRow, schema : StructType) : GenericInternalRow = {
    new GenericInternalRow(internalRow.toSeq(schema).toArray)
  }

}
