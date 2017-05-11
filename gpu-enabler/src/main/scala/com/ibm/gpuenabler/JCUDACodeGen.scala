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

package com.ibm.gpuenabler

import java.io.{File, PrintWriter}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext}
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.gpuenabler.CUDAUtils._

/**
  * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[InternalRow]].
  */
object JCUDACodeGen extends _Logging {

  val GPUINPUT  = 1
  val GPUOUTPUT = 2
  val RDDOUTPUT = 4
  val CONST     = 8

  case class Variable(colName:String,
                      varType : Int,
                      dataType: DataType,
                      inSchemaIdx : Int,
                      outSchemaIdx : Int,
                      var length : Long,
                      outputSize: Long,
                      ctx : CodegenContext) {

    //TODO use case class

    if (is(GPUOUTPUT))
      if (is(GPUINPUT)) assume(false, "A column cannot be in both GPUINPUT & GPUOUTPUT")

    if (is(GPUOUTPUT))
      if (!is(RDDOUTPUT)) assume(false, "GPU OUTPUT column must be RDDOUT to get type details :: "+ colName)

    val codeStmt = scala.collection.mutable.Map.empty[String, String]

    def is(t: Int): Boolean = (varType & t) > 0

    var boxType: String = ctx.boxedType(dataType)
    var javaType: String = ctx.javaType(dataType)
    var isArray = false
    var arrayType: DataType = _
    var size = ""
    var hostVariableName = ""
    var deviceVariableName = ""

    varType match {
      case _ if is(GPUINPUT) || is(CONST) => 
        hostVariableName = s"gpuInputHost_$colName"
        deviceVariableName = s"gpuInputDevice_$colName"
      case _ if is(GPUOUTPUT) => 
        hostVariableName = s"gpuOutputHost_$colName"
        deviceVariableName = s"gpuOutputDevice_$colName"
      case _ if is(RDDOUTPUT) => 
        hostVariableName = s"directCopyHost_$colName"
    }

    if(is(CONST))
      size = s"Math.abs($length) * Sizeof.${javaType.toUpperCase()}"
    else {
      dataType match {
        case ArrayType(d, _) =>
          isArray = true
          arrayType = d
          boxType = ctx.boxedType(d)
          javaType = ctx.javaType(d)
          if (outputSize != 0 && is(GPUOUTPUT))
            size = s"$outputSize * Sizeof.${javaType.toUpperCase()} * ${hostVariableName}_numCols"
          else
            size = s"numElements * Sizeof.${javaType.toUpperCase()} * ${hostVariableName}_numCols"
        case _ => 
          if (outputSize != 0 && is(GPUOUTPUT))
            size = s"$outputSize * Sizeof.${javaType.toUpperCase()}"
          else
            size = s"numElements * Sizeof.${javaType.toUpperCase()}"
      }
    }

    if(boxType.eq("Integer")) boxType = "Int"
    if(boxType.eq("Byte")) boxType = ""

    codeStmt += "declareHost" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1)) {
        s"""
         |private ByteBuffer $hostVariableName;
         |private CUdeviceptr pinMemPtr_$colName;
         |${if(isArray) s"private int ${hostVariableName}_numCols;" else ""}
           """.stripMargin
      }
      else if (is(RDDOUTPUT)){
        if (isArray)
          s"""
            |private $javaType $hostVariableName[][];
            |private int ${hostVariableName}_numCols;
           """.stripMargin
        else
          s"private $javaType $hostVariableName[];\n"
      }
      else ""
    }

    codeStmt += "declareDevice" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1))
        s"private CUdeviceptr $deviceVariableName;\n"
      else
        ""
    }

    codeStmt += "allocateHost" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1))
        s"""|pinMemPtr_$colName = new CUdeviceptr();
            |${
              if (isArray) {
                if (is(GPUINPUT))
                  s"""
                    |if ( !((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}"))
                    |  ${hostVariableName}_numCols = r.getArray($inSchemaIdx).numElements();
                    |""".stripMargin
                 else
                  s"${hostVariableName}_numCols = $length;"
               }
               else ""
            }
            |if ( !${is(GPUINPUT)} || !((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
            | cuMemAllocHost(pinMemPtr_$colName, $size);
            | $hostVariableName = pinMemPtr_$colName.getByteBuffer(0,$size).order(ByteOrder.LITTLE_ENDIAN);
            |}
          """.stripMargin

      else if(is(RDDOUTPUT)) {
        if (isArray)
          s"""
           |${hostVariableName}_numCols = r.getArray($inSchemaIdx).numElements();
           |$hostVariableName = new $javaType[numElements][${hostVariableName}_numCols];
           |
           """.stripMargin
        else
          s"$hostVariableName = new $javaType[numElements];\n"
      }
      else
        ""
    }

    codeStmt += "allocateDevice" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1))
        s"""|
            |if ( !${is(GPUINPUT)} || !((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
            | $deviceVariableName = new CUdeviceptr();
            | cuMemAlloc($deviceVariableName, $size);
            | ${if (is(GPUOUTPUT)) s"cuMemsetD32Async($deviceVariableName, 0, $size / 4, cuStream);" else ""}
            |} else { 
            | $deviceVariableName = (CUdeviceptr)inputCMap.get(blockID+"gpuOutputDevice_${colName}");
            |} 
           """.stripMargin
      else
       ""
    }

    codeStmt += "checkLoop" -> {
      if (is(GPUINPUT)) {
          s""" || !inputCMap.containsKey(blockID + "gpuOutputDevice_${colName}") """
      } else 
        ""
    }

    codeStmt += "readFromInternalRow" -> {
      if (is(GPUINPUT)) {
        if (isArray)
          s"""
           |if (!((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
           |  $javaType tmp_${colName}[] = r.getArray($inSchemaIdx).to${boxType}Array();
           |  for(int j = 0; j < gpuInputHost_${colName}_numCols; j ++)
           |    ${hostVariableName}.put$boxType(tmp_${colName}[j]);
           |} 
        """.stripMargin
        else
          s"""
             |if (!((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
             |  ${hostVariableName}.put$boxType(${ctx.getValue("r", dataType, inSchemaIdx.toString)});
             |} 
           """.stripMargin
      }
      else if(is(GPUOUTPUT))
        ""
      else if(is(RDDOUTPUT)) {
        dataType match {
        case StringType =>
          s"$hostVariableName[i] = ${ctx.getValue("r", dataType, inSchemaIdx.toString)}.clone();\n"
        case ArrayType(d,_) =>
          s""" | $javaType tmp_${colName}[] = r.getArray($inSchemaIdx).to${boxType}Array();
               | for(int j=0; j<${hostVariableName}_numCols;j++)
               |    ${hostVariableName}[i][j] = tmp_${colName}[j];
          """.stripMargin
        case _ =>
          s"${hostVariableName}[i] = ${ctx.getValue("r", dataType, inSchemaIdx.toString)};\n"
        }
      }
      else
        ""
    }
    codeStmt += "readFromConstArray" -> {
      if((is(CONST) && length != -1)) {
        s"""
         | for(int i = 0; i<$length; i++ ) {
         |   $hostVariableName.put$boxType((($javaType[])refs[$colName])[i]);
         | }
       """.stripMargin
        }
      else
        ""
    }

    codeStmt += "rewind" -> {
      if(is(GPUINPUT) && is(GPUOUTPUT))
        s"""
           |${hostVariableName}.rewind();
           |""".stripMargin
      else
        ""
    }

    codeStmt += "flip" -> {
      if(is(GPUINPUT) || (is(CONST) && length != -1))
        s"""
           |if (!((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
           |  ${hostVariableName}.flip();
           |}
          """.stripMargin
      else
        ""
    }

    codeStmt += "memcpyH2D" ->{
      if(is(GPUINPUT) || (is(CONST) && length != -1))
        s"""|if (!((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
            |  cuMemcpyHtoDAsync($deviceVariableName,
            |      Pointer.to($hostVariableName),
            |      $size, cuStream);
            |}
        """.stripMargin
      else
        ""
    }

    codeStmt += "kernel-param" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        if (!is(CONST) || length != -1)
          s",Pointer.to($deviceVariableName)\n"
        else  // Directly pass the constant values for primitives.
          s",Pointer.to(new ${javaType}[]{(${ctx.boxedType(dataType)})refs[$colName]})\n"
      else
        ""
    }

    codeStmt += "memcpyD2H" -> {
      // TODO : Evaluate for performance;
      if(is(GPUOUTPUT)  || (is(GPUINPUT) && is(RDDOUTPUT)))
        s"cuMemcpyDtoHAsync(Pointer.to(${hostVariableName}), $deviceVariableName, $size, cuStream);\n"
      else
        ""
    }

    // Device memory will be freed if not cached.
    codeStmt += "FreeDeviceMemory" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1)) {
        if (is(GPUOUTPUT)) {
          s"""| if (((cached & 1) > 0)) {
            |   outputCMap.putIfAbsent(blockID+"${deviceVariableName.replace("_out_", "_in_")}", $deviceVariableName);
            | } else {
            |   cuMemFree($deviceVariableName);
            | }
         """.stripMargin
        }  else if (is(GPUINPUT)) {
          s"""|if (((cached & 2) > 0)) {
             | inputCMap.putIfAbsent(blockID+"${deviceVariableName.replace("gpuInputDevice_", "gpuOutputDevice_")}", $deviceVariableName);
             |} else {
             |  cuMemFree($deviceVariableName);
             |}
           """.stripMargin
        } else {
          s"""|cuMemFree($deviceVariableName);
         """.stripMargin
        }
      }else
        ""
    }

    //Host Memory will be freed only after iterator completes.
    codeStmt += "FreeHostMemory" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1))
        s"""
           |if (!((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
           |  cuMemFreeHost(pinMemPtr_$colName);
           |}
           |""".stripMargin
      else
        ""
    }

    codeStmt += "writeToInternalRow" -> {
      if(is(RDDOUTPUT)) {
        if(is(GPUINPUT) || is(GPUOUTPUT)) {
          if(isArray)
            s"""
               |int tmpCursor_${colName} = holder.cursor;
               |arrayWriter.initialize(holder,${hostVariableName}_numCols,${dataType.defaultSize});
               |for(int j=0;j<${hostVariableName}_numCols;j++)
               |  arrayWriter.write(j, ${hostVariableName}.get$boxType());
               |rowWriter.setOffsetAndSize(${outSchemaIdx}, tmpCursor_${colName}, holder.cursor - tmpCursor_${colName});
               |rowWriter.alignToWords(holder.cursor - tmpCursor_${colName});
            """.stripMargin
          else
            s"rowWriter.write(${outSchemaIdx},${hostVariableName}.get$boxType());\n"
        }
        else {
          dataType match {
            case ArrayType(d, _) =>
              s"""
                 |int tmpCursor${colName} = holder.cursor;
                 |arrayWriter.initialize(holder,${hostVariableName}_numCols,${dataType.defaultSize});
                 |for(int j=0;j<${hostVariableName}_numCols;j++)
                 |  arrayWriter.write(j, ${hostVariableName}[idx][j]);
                 |rowWriter.setOffsetAndSize(${outSchemaIdx}, tmpCursor${colName}, holder.cursor - tmpCursor${colName});
                 |rowWriter.alignToWords(holder.cursor - tmpCursor${colName});
             """.stripMargin
            case _ =>
              s"rowWriter.write(${outSchemaIdx},${hostVariableName}[idx]);\n"
          }
        }
      }
      else ""
    }
  }

  def createVariables(inputSchema : StructType, outputSchema : StructType,
                      cf : DSCUDAFunction, args : Array[Any],
                      outputArraySizes: Seq[Int], ctx : CodegenContext): Array[Variable] = {
    // columns to be copied from inputRow to outputRow without gpu computation.
    val variables = ArrayBuffer.empty[Variable]

    def findSchemaIndex(schema: StructType, colName: String) =
      getAttributes(schema).indexWhere(a => a.name.equalsIgnoreCase(colName))

    cf._inputColumnsOrder.foreach {
      x => {
        val inIdx = findSchemaIndex(inputSchema, x)
        assume(inIdx >= 0, s"$inIdx $x not available in input Schema")
        val outIdx = findSchemaIndex(outputSchema, x)
        val outCol = cf._outputColumnsOrder.exists(_.equals(x))
        variables += Variable("in_"+x,
          GPUINPUT | {
            if (outIdx > -1 && !outCol) RDDOUTPUT else 0
          }
          //  | {
          //  if (outCol) GPUOUTPUT else 0
          // }
          ,
          inputSchema(inIdx).dataType,
          inIdx,
          outIdx,
          1,  // For Array input Variables, length will be determined at runtime.
          cf.outputSize.getOrElse(0), // Will be applicable for GPUOUTPUT
          ctx
        )
      }
    }

    if (outputArraySizes.isEmpty) {
      cf._outputColumnsOrder.foreach {
        x => {
          // val inCol = cf._inputColumnsOrder.exists(_.equals(x))
          // if (! inCol) { // If args is found in GPUINPUT, skip this variable;
            val outIdx = findSchemaIndex(outputSchema, x)

            // GPU OUTPUT variables must be in the output -- TODO may need to relax
            assume(outIdx >= 0)

            variables += Variable("out_"+x,
              GPUOUTPUT | RDDOUTPUT,
              outputSchema(outIdx).dataType,
              -1,
              outIdx,
              1,
              cf.outputSize.getOrElse(0),
              ctx)
          // }
        }
      }
    } else {
      assert(cf._outputColumnsOrder.length == outputArraySizes.length)
      cf._outputColumnsOrder.zip(outputArraySizes).foreach(col => {
        // val inCol = cf._inputColumnsOrder.exists(_.equals(col._1))
        // if (! inCol) { // If args is found in GPUINPUT, skip this variable;
	  val outIdx = findSchemaIndex(outputSchema, col._1)

          // GPU OUTPUT variables must be in the output -- TODO may need to relax
          assume(outIdx >= 0)

          variables += Variable("out_"+col._1,
            GPUOUTPUT | RDDOUTPUT,
            outputSchema(outIdx).dataType,
            -1,
            outIdx,
            col._2, // User provided Array output size
            cf.outputSize.getOrElse(0),
            ctx)
        // }
      })
    }

    var cnt = 0
    args.foreach {
      x => {
        val dtype =
          x match {
            case y: Array[Long] => (LongType, y.length)
            case y: Array[Int] => (IntegerType, y.length)
            case y: Array[Double] => (DoubleType, y.length)
            case y: Array[Float] => (FloatType, y.length)
            case y: Array[Char] => (ByteType, y.length)
            case _: Long => (LongType, -1) //  -1 to indicate primitives
            case _: Int => (IntegerType, -1)
            case _: Double => (DoubleType, -1)
            case _: Float => (FloatType, -1)
            case _: Char => (ByteType, -1)
	    case _: Short => (ShortType, -1)
          }

        variables += Variable(cnt.toString,
          CONST,
          dtype._1,
          -1,
          -1,
          dtype._2,  // array length of the const arguments.
          0,         // Output Size is not applicable for const arguments.
          ctx
        )
        cnt += 1
      }
    }

    // There could be some column which is neither in GPUInput nor GPUOutput
    // It would be directly copied from schema.
    getAttributes(outputSchema).foreach {
      x => {
        val outCol = cf._outputColumnsOrder.exists(_.equals(x.name))
	val inCol = cf._inputColumnsOrder.exists(_.equals(x.name))
        if (!outCol && !inCol) {
          variables += Variable(x.name,
            RDDOUTPUT,
            x.dataType,
            findSchemaIndex(inputSchema,x.name),
            findSchemaIndex(outputSchema,x.name),
            -1, cf.outputSize.getOrElse(0),
            ctx
          )
        }
      }
    }

    variables.toArray
  }

  def getStmt(variables : Array[Variable], stmtTypes: Seq[String],spacer : String): String = {
    val codeBody =  new StringBuilder

    variables.foreach { v =>
      stmtTypes.foreach {
        stmtType => codeBody.append( v.codeStmt.getOrElse(stmtType,""))
      }
      codeBody.append(spacer)
    }
    codeBody.dropRight(1).toString()
  }

  def getUserDimensions(numElements: Int): (Int, Array[Int], Array[Int]) = {
    val stagesCount: Int = localCF.stagesCount match {
      case Some(getCount) => getCount(numElements)
      case None => 1
    }

    val gpuBlockSizeList = Array.fill(stagesCount){0}
    val gpuGridSizeList = Array.fill(stagesCount){0}

    (0 to stagesCount-1).foreach(idx => {
      val (gpuGridSize, gpuBlockSize) = localCF.dimensions match {
        case Some(computeDim) => computeDim(numElements, idx)
        case None => (0,0) // Compute this in the executor nodes
      }
      gpuBlockSizeList(idx) = gpuBlockSize
      gpuGridSizeList(idx) = gpuGridSize
    })

    (stagesCount, gpuGridSizeList, gpuBlockSizeList)
  }

  var localCF: DSCUDAFunction = _

  def generate(inputSchema : StructType, outputSchema : StructType,
                   cf : DSCUDAFunction, args: Array[Any],
               outputArraySizes: Array[Int]) : JCUDACodegenIterator = {

    val ctx = new CodegenContext()
    localCF = cf

    val variables = createVariables(inputSchema,outputSchema,cf,args,outputArraySizes, ctx)
    val debugMode = !SparkEnv.get.conf.get("DebugMode","").isEmpty
    if(debugMode)
      println("Compile Existing File - DebugMode")
   // else
      // println("Generate Code")



    val codeBody =
      s"""
        |package org.apache.spark.sql.gpuenabler; // REMOVE
        |import jcuda.Pointer;
        |import jcuda.Sizeof;
        |import jcuda.driver.CUdeviceptr;
        |import jcuda.driver.CUdevice_attribute;
        |import jcuda.driver.CUfunction;
        |import jcuda.driver.CUmodule;
        |import jcuda.driver.CUdevice;
        |import jcuda.driver.JCudaDriver;
        |import jcuda.runtime.JCuda;
        |import jcuda.driver.CUstream;
        |import jcuda.runtime.cudaStream_t;
        |import org.apache.spark.sql.catalyst.InternalRow;
        |import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
        |import com.ibm.gpuenabler.JCUDACodegenIterator;
        |import org.apache.spark.unsafe.types.UTF8String;
        |
        |import java.nio.*;
        |import java.util.Iterator;
        |import static jcuda.driver.JCudaDriver.*;
        |import com.ibm.gpuenabler.GPUSparkEnv;
        |import java.util.Map;
        |import java.util.List;
        |
        |public class GeneratedCode_${cf.funcName} { // REMOVE
	|   // Handle to call from compiled source
	|   public JCUDACodegenIterator generate(Object[] references) {
	|     JCUDACodegenIterator j = new JCUDAIteratorImpl();
	|     return j;
	|   }
	| 
        |  class JCUDAIteratorImpl extends JCUDACodegenIterator {
        |    //Static variables
        |    private Object[] references;
        |    private UnsafeRow result;
        |    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
        |    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
        |    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter;
        |    private Iterator<InternalRow> inpitr = null;
        |    private boolean processed = false;
        |    private boolean freeMemory = true;
        |    private int idx = 0;
        |    private int numElements = 0;
        |    private int hasNextLoop = 0;
        |    private Object refs[];
        |
        |    // cached : 0 - NoCache;
        |    // 1 - DS is cached; Hold GPU results in GPU
        |    // 2 - child DS is cached; Use GPU results stored by child for input parameters
        |    private int cached = 0;
        |    private int blockID = 0;
        |    private List<Map<String,CUdeviceptr>> gpuPtrs;
        |    private Map<String, CUdeviceptr> inputCMap;
        |    private Map<String, CUdeviceptr> outputCMap;
        |
        |    private int[] blockSizeX;
        |    private int[] gridSizeX;
        |    private int stages;
        |
        |    ${getStmt(variables,List("declareHost","declareDevice"),"\n")}
        |
        |    public JCUDAIteratorImpl() {
        |        result = new UnsafeRow(${getAttributes(outputSchema).length});
        |        this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
        |        this.rowWriter =
        |           new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, ${getAttributes(outputSchema).length});
        |        arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
        |    }
        |
        |    public void init(Iterator<InternalRow> inp, Object inprefs[], int size, int cached,
        |             List<Map<String,CUdeviceptr>> gpuPtrs, int blockID, int[] userGridSizes, int[] userBlockSizes, int stages) {
        |        inpitr = inp;
        |        numElements = size;
        |        hasNextLoop = ${cf.outputSize.getOrElse("numElements")};
        |
        |        refs = inprefs;
        |
        |        this.cached = cached;
        |        this.gpuPtrs = gpuPtrs;
        |        this.blockID = blockID;
        |
        |        gridSizeX = userGridSizes;
        |        blockSizeX = userBlockSizes;
        |        this.stages = stages;
        |
        |        if (((cached & 1) > 0)) {
        |           outputCMap = (Map<String, CUdeviceptr>)gpuPtrs.get(0);
        |        }
        |
        |        if (((cached & 2) > 0)) {
        |           inputCMap = (Map<String, CUdeviceptr>) gpuPtrs.get(1);
        |        }
        |    }
        |
        |    public boolean hasNext() {
        |        if(!processed) {
        |           processGPU();
        |           processed = true;
        |        }
        |        else if(idx >= hasNextLoop) {
        |           if(freeMemory) {
        |              freePinnedMemory();
        |              freeMemory=false;
        |           }
        |           return false;
        |        }
        |        return true;
        |    }
        |
        |    private void freePinnedMemory() {
        |        ${getStmt(variables,List("FreeHostMemory"),"")}
        |    }
        |
        |    private void allocateMemory(InternalRow r, CUstream cuStream ) {
        |
        |       // Allocate Host and Device variables
        |       ${getStmt(variables,List("allocateHost","allocateDevice"),"\n")}
        |    }
        |
        |    public void processGPU() {
        |       
        |       CUmodule module = GPUSparkEnv.get().cudaManager().getModule("${cf.resource}");
        |       ${ if (cf.stagesCount.isEmpty) {
		 s"""| blockSizeX = new int[1];
		     | gridSizeX = new int[1];
		     |
		     | CUdevice dev = new CUdevice();
		     | JCudaDriver.cuCtxGetDevice(dev);
		     | int dim[] = new int[1];
		     | JCudaDriver.cuDeviceGetAttribute(dim, CUdevice_attribute.CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_X, dev);
		     |
		     | blockSizeX[0] = dim[0];
		     | gridSizeX[0] = (int) Math.ceil((double) numElements / blockSizeX[0]);
		""".stripMargin
                } else "" }
        |       // Obtain a function pointer to the ${cf.funcName} function.
        |       CUfunction function = new CUfunction();
        |       cuModuleGetFunction(function, module, "${cf.funcName}");
        |
        |       cudaStream_t stream = new cudaStream_t();
        |       JCuda.cudaStreamCreateWithFlags(stream, JCuda.cudaStreamNonBlocking);
        |       CUstream cuStream = new CUstream();
        |
	|       inpitr.hasNext();
	|       Boolean enterLoop = true;
	|       if (!((cached & 2) > 0) ${getStmt(variables,List("checkLoop"),"")} ) {
	|         enterLoop = true;
	|       } else {
	|         enterLoop = false;
	|       }
	|
	|       if (enterLoop){
        |       // Fill GPUInput/Direct Copy Host variables
        |       for(int i=0; inpitr.hasNext();i++) {
        |          InternalRow r = (InternalRow) inpitr.next();
        |          if (i == 0)  allocateMemory(r, cuStream);
        |          ${getStmt(variables,List("readFromInternalRow"),"")}
        |       }
	|       }
        |
        |      ${getStmt(variables,List("readFromConstArray"),"")}
        |
        |       // Flip buffer for read
        |       ${getStmt(variables,List("flip"),"")}
        |
        |       // Copy data from Host to Device
        |       ${getStmt(variables,List("memcpyH2D"),"")}
        |       cuCtxSynchronize();
        |
        | ${
            if (cf.stagesCount.isEmpty) {
              s"""
                 |  Pointer kernelParameters = Pointer.to(
                 |    Pointer.to(new int[]{numElements})
                 |    ${getStmt(variables, List("kernel-param"), "")}
                 |  );
                 |
                 |  // Call the kernel function.
                 |  cuLaunchKernel(function,
                 |    gridSizeX[0], 1, 1,      // Grid dimension
                 |    blockSizeX[0], 1, 1,      // Block dimension
                 |    0, cuStream,               // Shared memory size and stream
                 |    kernelParameters, null // Kernel- and extra parameters
                 |  );
               """.stripMargin
            } else
              s"""
                 | for (int stage = 0; stage < stages; stage++) {
                 |    Pointer kernelParameters = Pointer.to(
                 |      Pointer.to(new int[]{numElements})
                 |      ${getStmt(variables, List("kernel-param"), "")}
                 |      ,Pointer.to(new int[]{stage})   // Stage number
                 |      ,Pointer.to(new int[]{stages})   // Total Stages
                 |    );
                 |
                 |    // Call the kernel function.
                 |    cuLaunchKernel(function,
                 |      gridSizeX[stage], 1, 1,      // Grid dimension
                 |      blockSizeX[stage], 1, 1,      // Block dimension
                 |      0, cuStream,               // Shared memory size and stream
                 |      kernelParameters, null // Kernel- and extra parameters
                 |    );
                 |
                 | }
                 |
               """.stripMargin
        }
        |        
        |        ${getStmt(variables,List("memcpyD2H"),"")}
        |        cuCtxSynchronize();
        |        // Rewind buffer for read for GPUINPUT & GPUOUTPUT 
        |        ${getStmt(variables,List("rewind"),"")}
        |        ${getStmt(variables,List("FreeDeviceMemory"),"")}
        |        JCuda.cudaStreamDestroy(stream);
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

    val fpath = s"/tmp/GeneratedCode_${cf.funcName}.java"

    def writeToFile(codeBody : String): Unit = {
      val code = new CodeAndComment(codeBody,ctx.getPlaceHolderToComments())
      val pw = new PrintWriter(new File(fpath))
      pw.write(CodeFormatter.format(code))
      pw.close()
      // println("The generated file path = " + fpath)
    }

    if(debugMode)
      generateFromFile(fpath)
    else {
      writeToFile(codeBody)

      val code = codeBody.split("\n").filter(!_.contains("REMOVE")).map(_ + "\n").mkString
      val p = CodeGenerator.compile(new CodeAndComment(code, ctx.getPlaceHolderToComments())).
        generate(ctx.references.toArray).asInstanceOf[JCUDACodegenIterator]

      p
    }
  }

  def generateFromFile(fpath : String) : JCUDACodegenIterator = {

    val ctx = new CodegenContext()

    val c = scala.io.Source.fromFile(fpath).getLines()
    val codeBody = c.filter(!_.contains("REMOVE")).map(x => x+"\n").mkString

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDACodegenIterator]
    p
  }

}

