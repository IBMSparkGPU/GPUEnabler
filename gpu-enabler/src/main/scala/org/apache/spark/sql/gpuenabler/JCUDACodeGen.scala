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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer

/**
  * Interface for generated predicate
  */
abstract class JCUDAInterface {
  def hasNext() : Boolean
  def next() : InternalRow
  def init(itr : java.util.Iterator[InternalRow], size : Int)
}

/**
  * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[InternalRow]].
  */
object JCUDACodeGen extends Logging {

  val GPUINPUT  = 1
  val GPUOUTPUT = 2
  val RDDOUTPUT = 4

  case class Variable(colName:String,
                      varType : Int,
                      dataType: DataType,
                      inSchemaIdx : Int,
                      outSchemaIdx : Int,
                      ctx : CodegenContext) {

    //TODO use case class

    val codeStmt = scala.collection.mutable.Map.empty[String,String]

    def is(t : Int) = ((varType & t) > 0)
    val boxType = ctx.boxedType(dataType);
    val javaType = ctx.javaType(dataType);
    val size = s"numElements * Sizeof.${javaType.toUpperCase()}"

    varType match {
      case _ if is(GPUINPUT) => {
        val hostVariableName = s"gpuInputHost_$colName"
        val deviceVariableName = s"gpuInputDevice_$colName"

        codeStmt += "declareHost" ->
          s"private ByteBuffer ${hostVariableName};\n"

        codeStmt += "declareDevice" ->
          s"private CUdeviceptr ${deviceVariableName};\n"

        codeStmt += "allocateHost" ->
          s"""|myPointer myptr_$colName = new myPointer();
              |cuMemAllocHost(myptr_$colName, $size);
              |$hostVariableName =
              |     myptr_$colName.getByteBuffer(0,
              |                $size).order(ByteOrder.LITTLE_ENDIAN);
           """.stripMargin

        codeStmt += "allocateDevice" ->
          s"""|$deviceVariableName = new CUdeviceptr();
              |cuMemAlloc($deviceVariableName, $size);
           """.stripMargin

        codeStmt += "readFromInternalRow" ->
          s"${hostVariableName}.put$boxType(${ctx.getValue("r", dataType, inSchemaIdx.toString)});\n"

        codeStmt += "flip" ->
          s"${hostVariableName}.flip();\n"

        codeStmt += "memcpyH2D" ->
          s""" |  cuMemcpyHtoD($deviceVariableName,
               |      Pointer.to($hostVariableName),
               |      $size);
               |      """.stripMargin

        codeStmt += "kernel-param" -> s",Pointer.to($deviceVariableName)\n"

        codeStmt += "Free" ->
          s""" |cuMemFreeHost(myptr_$colName);
               |cuMemFree($deviceVariableName);
           """.stripMargin

        if (!is(GPUOUTPUT) && is(RDDOUTPUT))
          codeStmt += "writeToInternalRow" ->
            s"rowWriter.write(${outSchemaIdx},${hostVariableName}.get$boxType(idx));\n"
      }
      case _ if is(GPUOUTPUT) => {
        val hostVariableName = s"gpuOutputHost_$colName"
        val deviceVariableName = s"gpuOutputDevice_$colName"

        codeStmt += "declareHost" ->
          s"private ${ctx.javaType(dataType)} ${hostVariableName}[];\n"

        codeStmt += "declareDevice" ->
          s"private CUdeviceptr ${deviceVariableName};\n"

        codeStmt += "allocateHost" ->
          s"$hostVariableName = new $javaType[numElements];\n"

        codeStmt += "allocateDevice" ->
          s"""|$deviceVariableName = new CUdeviceptr();
             |cuMemAlloc($deviceVariableName, $size);
           """.stripMargin

        codeStmt += "memcpyD2H" ->
          s"cuMemcpyDtoH(Pointer.to($hostVariableName), $deviceVariableName, $size);\n"

        codeStmt += "kernel-param" -> s",Pointer.to($deviceVariableName)\n"

        if (is(RDDOUTPUT))
          codeStmt += "writeToInternalRow" ->
            s"rowWriter.write(${outSchemaIdx},${hostVariableName}[idx]);\n"

        codeStmt += "Free" -> s"cuMemFree($deviceVariableName);\n"
      }
      case _ if is(RDDOUTPUT) => {
        val hostVariableName = s"directCopyHost_$colName"

        codeStmt += "declareHost" ->
          s"private $javaType ${hostVariableName}[];\n"

        codeStmt += "declareDevice" ->
          ""
        codeStmt += "allocateHost" ->
          s"$hostVariableName = new $javaType[numElements];\n"

        codeStmt += "readFromInternalRow" -> {
          dataType match {
            case StringType =>
              s"$hostVariableName[i] = ${ctx.getValue("r", dataType, inSchemaIdx.toString)}.clone();\n"
            case _ =>
              s"${hostVariableName}[i] = ${ctx.getValue("r", dataType, inSchemaIdx.toString)};\n"
          }
        }

        codeStmt += "writeToInternalRow" ->
          s"rowWriter.write(${outSchemaIdx},${hostVariableName}[idx]);\n"
      }
    }

  }

  def createVariables(inputSchema : StructType, outputSchema : StructType,
               cf : CudaFunc, ctx : CodegenContext) = {
    // columns to be copied from inputRow to outputRow without gpu computation.
    val variables = ArrayBuffer.empty[Variable]

    def findSchemaIndex(schema : StructType, colName : String) =
      schema.toAttributes.indexWhere(a => a.name.equalsIgnoreCase(colName))

    cf.inputArgs.foreach {
      x => {
        val inIdx = findSchemaIndex(inputSchema, x.name)
        assume(inIdx >= 0, s"$inIdx ${x.name} not available in input Schema")
        val outIdx = findSchemaIndex(outputSchema, x.name)
        variables += Variable(x.name,
          GPUINPUT | { if (outIdx > 1) RDDOUTPUT else 0 },
          inputSchema(inIdx).dataType,
          inIdx,
          outIdx,
          ctx
        )
      }
    }

    cf.outputArgs.foreach {
      x => {
        val outIdx = findSchemaIndex(outputSchema, x.name)

        // GPU OUTPUT variables must be in the output -- TODO may need to relax
        assume(outIdx >= 0)

        variables += Variable(x.name,
          GPUOUTPUT|RDDOUTPUT,
          outputSchema(outIdx).dataType,
          -1,
          outIdx,
          ctx)
      }
    }


    // There could be some column which is neither in GPUInput nor GPUOutput
    // It would be directly copied from schema.
    outputSchema.toAttributes.foreach {
      x =>
        if (variables.find(v => v.colName.equals(x.name)).isEmpty) {
          variables += Variable(x.name,
            RDDOUTPUT,
            x.dataType,
            findSchemaIndex(inputSchema,x.name),
            findSchemaIndex(outputSchema,x.name),
            ctx
          )

        }
    }
    variables.toArray
  }

  def getStmt(variables : Array[Variable], stmtTypes: Seq[String],spacer : String) = {
    val codeBody =  new StringBuilder

    variables.foreach { v =>
      stmtTypes.foreach {
        stmtType => codeBody.append( v.codeStmt.getOrElse(stmtType,""))
      }
      codeBody.append(spacer)
    }
    codeBody.dropRight(1).toString()
  }


  def generate(inputSchema : StructType, outputSchema : StructType,
                   cf : CudaFunc) : JCUDAInterface = {

    val ctx = new CodegenContext()

    val variables = createVariables(inputSchema,outputSchema,cf,ctx)


    val codeBody =
      s"""
        |package org.apache.spark.sql.gpuenabler; // REMOVE
        |import jcuda.Pointer;
        |import jcuda.Sizeof;
        |import jcuda.driver.CUdeviceptr;
        |import jcuda.driver.CUfunction;
        |import jcuda.driver.CUmodule;
        |import org.apache.spark.sql.catalyst.InternalRow;
        |import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
        |import org.apache.spark.sql.gpuenabler.JCUDAInterface;
        |import org.apache.spark.unsafe.types.UTF8String;
        |
        |import java.nio.*;
        |import java.util.Iterator;
        |import static jcuda.driver.JCudaDriver.*;
        |import com.ibm.gpuenabler.GPUSparkEnv;
        |
        |public class GeneratedCode_${cf.func.fname} { // REMOVE
        |
        |    // Handle to call from compiled source
        |    public JCUDAInterface generate(Object[] references) {
        |        JCUDAInterface j = new myJCUDAInterface();
        |        return j;
        |    }
        |
        |    //Handle to directly call from JCUDAVecAdd instance for debugging.
        |    public JCUDAInterface generate() {
        |        JCUDAInterface j = new myJCUDAInterface();
        |        return j;
        |    }
        |
        |    class myPointer extends CUdeviceptr {
        |       myPointer() {
        |         super();
        |       }
        |       public long getNativePointer() {
        |         return super.getNativePointer();
        |       }
        |    }
        |
        |   class myJCUDAInterface extends JCUDAInterface {
        |        //Static variables
        |        private Object[] references;
        |        private UnsafeRow result;
        |        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
        |        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
        |        private Iterator<InternalRow> inpitr = null;
        |        private boolean processed = false;
        |        private int idx = 0;
        |        private int numElements = 0;
        |
        |        ${getStmt(variables,List("declareHost","declareDevice"),"\n")}
        |
        |    public myJCUDAInterface() {
        |        result = new UnsafeRow(${outputSchema.toAttributes.length});
        |        this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
        |        this.rowWriter =
        |        new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, ${outputSchema.toAttributes.length});
        |    }
        |
        |    public void init(Iterator<InternalRow> inp, int size) {
        |        inpitr = inp;
        |        numElements = size;
        |    }
        |
        |    public boolean hasNext() {
        |        if(!processed) {
        |           processGPU();
        |           processed = true;
        |        }
        |        return idx < numElements;
        |    }
        |
        |    public void processGPU() {
        |
        |       CUmodule module = GPUSparkEnv.get().cudaManager().getModule("${cf.func.ptxPath}");
        |
        |       // Obtain a function pointer to the ${cf.func.fname} function.
        |       CUfunction function = new CUfunction();
        |       cuModuleGetFunction(function, module, "${cf.func.fname}");
        |
        |       // Allocate Host and Device variables
        |       ${getStmt(variables,List("allocateHost","allocateDevice"),"\n")}
        |
        |       // Fill GPUInput/Direct Copy Host variables
        |       for(int i=0; inpitr.hasNext();i++) {
        |          InternalRow r = (InternalRow) inpitr.next();
        |          ${getStmt(variables,List("readFromInternalRow"),"")}
        |       }
        |
        |       // Flip buffer for read
        |       ${getStmt(variables,List("flip"),"")}
        |
        |       // Copy data from Host to Device
        |       ${getStmt(variables,List("memcpyH2D"),"")}
        |
        |       Pointer kernelParameters = Pointer.to(
        |        Pointer.to(new int[]{numElements})
        |        ${getStmt(variables,List("kernel-param"),"")}
        |       );
        |
        |        // Call the kernel function.
        |        int blockSizeX = 256;
        |        int gridSizeX = (int) Math.ceil((double) numElements / blockSizeX);
        |
        |        cuLaunchKernel(function,
        |                gridSizeX, 1, 1,      // Grid dimension
        |                blockSizeX, 1, 1,      // Block dimension
        |                0, null,               // Shared memory size and stream
        |                kernelParameters, null // Kernel- and extra parameters
        |        );
        |
        |
        |        cuCtxSynchronize();
        |
        |        ${getStmt(variables,List("memcpyD2H"),"")}
        |
        |        ${getStmt(variables,List("Free"),"")}
        |    }
        |
        |    public InternalRow next() {
        |       holder.reset();
        |       rowWriter.zeroOutNullBytes();
        |       ${getStmt(variables,List("writeToInternalRow"),"")}
        |       result.setTotalSize(holder.totalSize());
        |       idx++;
        |       return (InternalRow) result;
        |    }
        |  }
        |} // REMOVE
      """.stripMargin

    def writeToFile(codeBody : String): Unit = {
      val code = new CodeAndComment(codeBody,ctx.getPlaceHolderToComments())
      val fpath = s"${Utils.homeDir}GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/GeneratedCode_${cf.func.fname}.java"
      val pw = new PrintWriter(new File(fpath))
      pw.write(CodeFormatter.format(code))
      pw.close
      println(CodeFormatter.format(code))
      println("The generated file path = " + fpath)

    }

    writeToFile(codeBody)

    val code = codeBody.split("\n").filter(!_.contains("REMOVE")).map(_ + "\n").mkString
    val p = CodeGenerator.compile(new CodeAndComment(code, ctx.getPlaceHolderToComments())).
      generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]

    if(Utils.homeDir.contains("madhusudanan"))
      //return generateFromFile
      return new JCUDAVecAdd().generate()
    else {
      return p;
    }

  }

  def generateFromFile : JCUDAInterface = {

    val ctx = new CodegenContext()

    val c = scala.io.Source.fromFile(s"${Utils.homeDir}/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    //val c = scala.io.Source.fromFile("/home/kmadhu/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    val codeBody = c.filter(x=> (!(x.contains("REMOVE")))).map(x => x+"\n").mkString

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]
    return p;
  }

}

