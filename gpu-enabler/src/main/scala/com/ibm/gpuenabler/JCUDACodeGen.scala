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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext}
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.sql.gpuenabler.CUDAUtils._

/**
  * Generates bytecode that can load/unload data & modules into GPU and 
  * launch kernel in GPU using native CUDA libraries.
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
                      var length : Int,
                      outputSize: Long,
                      ctx : CodegenContext) {

    //TODO use case class

    if (is(GPUOUTPUT))
      if (is(GPUINPUT)) 
        assume(false, "A column cannot be in both GPUINPUT & GPUOUTPUT")

    if (is(GPUOUTPUT))
      if (!is(RDDOUTPUT)) 
        assume(false, "GPU OUTPUT column must be RDDOUT to get type details :: "+ colName)

    val codeStmt = scala.collection.mutable.Map.empty[String, String]

    def is(t: Int): Boolean = (varType & t) > 0

    var boxType: String = ctx.boxedType(dataType)
    var javaType: String = ctx.javaType(dataType)
    var isArray = false
    var arrayType: DataType = _
    var size = ""
    var hostVariableName = ""
    var deviceVariableName = ""

    // Create variable names based on column names
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

    // Create variable's size based on column data type
    if(is(CONST)) {
      size = s"1L * Math.abs($length) * Sizeof.${javaType.toUpperCase()}"
    } else {
      dataType match {
        case ArrayType(d, _) =>
          isArray = true
          arrayType = d
          boxType = ctx.boxedType(d)
          javaType = ctx.javaType(d)
          if (outputSize != 0 && is(GPUOUTPUT)) {
            size = s"1L * $outputSize * Sizeof.${javaType.toUpperCase()} * ${hostVariableName}_colWidth"
          } else {
            size = s"1L * numElements * Sizeof.${javaType.toUpperCase()} * ${hostVariableName}_colWidth"
          }
        case _ => 
          if (outputSize != 0 && is(GPUOUTPUT)) {
            size = s"1L * $outputSize * Sizeof.${javaType.toUpperCase()}"
          } else {
            size = s"1L * numElements * Sizeof.${javaType.toUpperCase()}"
          }
      }
    }

    if(boxType.eq("Integer")) boxType = "Int"
    if(boxType.eq("Byte")) boxType = ""

    // Host Variable Declaration
    codeStmt += "declareHost" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1)) {
        s"""
         |private ByteBuffer $hostVariableName;
         |private CUdeviceptr pinMemPtr_$colName;
         |${if(isArray) s"private int ${hostVariableName}_colWidth;" else ""}
           """.stripMargin
      }
      else if (is(RDDOUTPUT)){
        if (isArray)
          s"""
            |private $javaType $hostVariableName[][];
            |private int ${hostVariableName}_colWidth;
           """.stripMargin
        else
          s"private $javaType $hostVariableName[];\n"
      }
      else ""
    }

    //Device Variable Declaration
    codeStmt += "declareDevice" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1))
        s"private CUdeviceptr $deviceVariableName;\n"
      else
        ""
    }

    // Host Variable Allocation; If data is cached, 
    // skip the host allocation only if its not part of output.
    codeStmt += "allocateHost" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1))
        s"""|pinMemPtr_$colName = new CUdeviceptr();
            |${
              if (isArray) {
                if (is(GPUINPUT))
                  s""" 
                    |if (!((cached & 2) > 0) || 
                    |    !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")  || ${is(RDDOUTPUT)}) {
                    |  if (r == null && inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) 
                    |   ${hostVariableName}_colWidth = ((CachedGPUMeta)inputCMap.get(blockID+"gpuOutputDevice_${colName}")).colWidth();
                    |  else
                    |   ${hostVariableName}_colWidth = r.getArray($inSchemaIdx).numElements();
                    |} 
                    |""".stripMargin
                 else
                  s"${hostVariableName}_colWidth = $length;"
               }
               else ""
            }
            |if ( !${is(GPUINPUT)} || !((cached & 2) > 0) ||
            |   !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}") || ${is(RDDOUTPUT)} ) { 
            | if (!((cached & 4) > 0) || ${is(GPUINPUT)} || ${is(CONST)} ) {
            |   assert (($size) < Integer.MAX_VALUE) : "Partition size too big to handle by cuMemAllocHost";
            |   cuMemAllocHost(pinMemPtr_$colName, $size);
            |   $hostVariableName = pinMemPtr_$colName.getByteBuffer(0,$size).order(ByteOrder.LITTLE_ENDIAN);
            | }
            |}
          """.stripMargin

      else if(is(RDDOUTPUT)) {
        if (isArray)
          s"""
           |${hostVariableName}_colWidth = r.getArray($inSchemaIdx).numElements();
           |$hostVariableName = new $javaType[numElements][${hostVariableName}_colWidth];
           |
           """.stripMargin
        else
          s"$hostVariableName = new $javaType[numElements];\n"
      }
      else
        ""
    }

    // Device Variable Allocation; If data is cached, retrieve the GPU devicePtrs
    // corresponding to it.
    codeStmt += "allocateDevice" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1))
        s"""|
            |if ( !${is(GPUINPUT)} || !((cached & 2) > 0)  ||
            |   !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
            | $deviceVariableName = new CUdeviceptr();
            | cuMemAlloc($deviceVariableName, $size);
            | ${if (is(GPUOUTPUT)) s"cuMemsetD32Async($deviceVariableName, 0, $size / 4, cuStream);" else ""}
            |} else {
            | $deviceVariableName = (CUdeviceptr)((CachedGPUMeta)inputCMap.get(blockID+"gpuOutputDevice_${colName}")).ptr();
            |} 
           """.stripMargin
      else
       ""
    }

    // If all input data to GPU is cached; we can skip iterating through the 
    // data retrieval from previous logical plan.
    codeStmt += "checkLoop" -> {
      if (is(GPUINPUT)) {
          s""" || !inputCMap.containsKey(blockID + "gpuOutputDevice_${colName}") """
      } else 
        ""
    }

    // Read from internal row based on the data type; If data is cached; skip it.
    codeStmt += "readFromInternalRow" -> {
      if (is(GPUINPUT)) {
        if (isArray)
          s"""
           |if (!((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
           |  $javaType tmp_${colName}[] = r.getArray($inSchemaIdx).to${boxType}Array();
           |  for(int j = 0; j < gpuInputHost_${colName}_colWidth; j ++)
           |    ${hostVariableName}.put$boxType(tmp_${colName}[j]);
           |} 
        """.stripMargin
        else
          s"""
             |if (!((cached & 2) > 0) || !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
             | ${hostVariableName}.put$boxType(${ctx.getValue("r", dataType, inSchemaIdx.toString)});
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
               | for(int j=0; j<${hostVariableName}_colWidth;j++)
               |    ${hostVariableName}[i][j] = tmp_${colName}[j];
          """.stripMargin
        case _ =>
          s"${hostVariableName}[i] = ${ctx.getValue("r", dataType, inSchemaIdx.toString)};\n"
        }
      }
      else
        ""
    }

    // Retrieve the free variable passed from user program
    codeStmt += "readFromConstArray" -> {
      if(is(CONST) && length != -1) {
        s"""
         | for(int i = 0; i<$length; i++ ) {
         |   $hostVariableName.put$boxType((($javaType[])refs[$colName])[i]);
         | }
       """.stripMargin
        }
      else
        ""
    }

    // Rewind the bytebuffer whenever required
    codeStmt += "rewind" -> {
      if(is(GPUINPUT) && is(GPUOUTPUT))
        s"""
           |${hostVariableName}.rewind();
           |""".stripMargin
      else
        ""
    }

    // Flip the bytebuffer whenever required
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

    // Copy Data from host to device memory
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

    // Wrap the device variable for creating kernel parameters
    codeStmt += "kernel-param" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        if (!is(CONST) || length != -1)
          s",Pointer.to($deviceVariableName)\n"
        else  // Directly pass the constant values for primitives.
          s",Pointer.to(new ${javaType}[]{(${ctx.boxedType(dataType)})refs[$colName]})\n"
      else
        ""
    }

    // Wrap the device variable for creating kernel parameters
    codeStmt += "kernel-param-mod" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        if (!is(CONST) || length != -1)
          s",$deviceVariableName\n"
        else  // Directly pass the constant values for primitives.
          s",(new ${javaType}[]{(${ctx.boxedType(dataType)})refs[$colName]})\n"
      else
        ""
    }

    // Copy Data from device to host memory; only for non-GPU only cached memory
    codeStmt += "memcpyD2H" -> {
      // TODO : Evaluate for performance;
      if(is(GPUOUTPUT)  || (is(GPUINPUT) && is(RDDOUTPUT)))
        s"""
           | if (!((cached & 4) > 0)) {
           | cuMemcpyDtoHAsync(Pointer.to(${hostVariableName}), $deviceVariableName, $size, cuStream); \n }
         """.stripMargin
      else
        ""
    }

    // Device memory will be freed if not cached.
    // GPUOUT : This LP should own this device memory; if cached , store it into my ptrs clean it later
    // GPUIN : child LP should own this device memory; if child is cached, store it in child ptrs,
    //         if child is cached and clean when child is unCached. If this variable is
    //         part of this plan's output(RDDOUTPUT) store it into my ptrs but not clean
    //         it as part of my unCached routines. But own it if the child is not cached.
    codeStmt += "FreeDeviceMemory" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || (is(CONST) && length != -1)) {
        if (is(GPUOUTPUT)) {
          s"""|
              | if (((cached & 1) > 0)) {
              |   own = true;
              |   ${if (isArray) {
                     s"colWidth = ${hostVariableName}_colWidth;"
                  } else {
                     "colWidth = 1;"
                  }}
              |   outputCMap.putIfAbsent(blockID+
              |    "${deviceVariableName.replace("_out_", "_in_")}", new CachedGPUMeta($deviceVariableName, own, colWidth));
              | } else {
              |   cuMemFree($deviceVariableName);
              | }
         """.stripMargin
        }  else if (is(GPUINPUT)) {
          s"""|
              | ${if (isArray) {
                     s"colWidth = ${hostVariableName}_colWidth;"
                  } else {
                     "colWidth = 1;"
                  }}
              | own = true;
              | if (((cached & 2) > 0)) {
              |   own = false;
              |   inputCMap.putIfAbsent(blockID
              |    +"${deviceVariableName.replace("gpuInputDevice_", "gpuOutputDevice_")}", new CachedGPUMeta($deviceVariableName, true, colWidth));
              | }
              | if (${is(RDDOUTPUT)} && (cached & 1) > 0) {
              |   outputCMap.putIfAbsent(blockID+
              |     "${deviceVariableName.replace("gpuInputDevice_", "gpuOutputDevice_")}", new CachedGPUMeta($deviceVariableName, own, colWidth));
              | } else if (own) {
              |   cuMemFree($deviceVariableName);
              | }
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
           |if (${is(RDDOUTPUT)} || !((cached & 2) > 0) ||
           |    !inputCMap.containsKey(blockID+"gpuOutputDevice_${colName}")) {
           |  cuMemFreeHost(pinMemPtr_$colName);
           |}
           |""".stripMargin
      else
        ""
    }

    // Create an Internal Row and populate it with the results from GPU
    codeStmt += "writeToInternalRow" -> {
      if(is(RDDOUTPUT)) {
        if(is(GPUINPUT) || is(GPUOUTPUT)) {
          if(isArray)
            s"""
               |int tmpCursor_${colName} = holder.cursor;
               |arrayWriter.initialize(holder,${hostVariableName}_colWidth,
               |  ${dataType.defaultSize});
               |for(int j=0;j<${hostVariableName}_colWidth;j++)
               |  arrayWriter.write(j, ${hostVariableName}.get$boxType());
               |rowWriter.setOffsetAndSize(${outSchemaIdx}, 
               |  tmpCursor_${colName}, holder.cursor - tmpCursor_${colName});
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
                 |arrayWriter.initialize(holder,${hostVariableName}_colWidth,
                 |  ${dataType.defaultSize});
                 |for(int j=0;j<${hostVariableName}_colWidth;j++)
                 |  arrayWriter.write(j, ${hostVariableName}[idx][j]);
                 |rowWriter.setOffsetAndSize(${outSchemaIdx}, tmpCursor${colName},
                 |  holder.cursor - tmpCursor${colName});
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
          } ,
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
        }
      }
    } else {
      assert(cf._outputColumnsOrder.length == outputArraySizes.length)
      cf._outputColumnsOrder.zip(outputArraySizes).foreach(col => {
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

  def getStmt(variables : Array[Variable], stmtTypes: Seq[String],
          spacer : String): String = {
    val codeBody =  new StringBuilder

    variables.foreach { v =>
      stmtTypes.foreach {
        stmtType => codeBody.append( v.codeStmt.getOrElse(stmtType,""))
      }
      codeBody.append(spacer)
    }
    codeBody.dropRight(1).toString()
  }

  def getUserDimensions(localCF: DSCUDAFunction, 
       numElements: Int): (Int, Array[Array[Int]], Array[Array[Int]], Int) = {
    val stagesCount: Int = localCF.stagesCount match {
      case Some(getCount) => getCount(numElements)
      case None => 1
    }

    val gpuBlockSizeList = Array.fill(stagesCount,3){0}
    val gpuGridSizeList = Array.fill(stagesCount,3){0}
    var gpuSharedMemory : Int = 0

    (0 until stagesCount).foreach(idx => {
      val (gpuGridSizeX : Int, gpuBlockSizeX: Int, gpuGridSizeY : Int, gpuBlockSizeY : Int, gpuGridSizeZ : Int, gpuBlockSizeZ: Int) = localCF.gpuParams match {
        case Some(gpuParamsCompute) =>
          gpuParamsCompute.dimensions match {
            case (computeDim) => computeDim(numElements, idx)
          }
        case None => (1,1,1,1,1,1) // Compute this in the executor nodes
      }

      assert(gpuGridSizeX >= 1 || gpuGridSizeY >= 1 || gpuGridSizeZ >= 1 || gpuBlockSizeX >= 1 || gpuBlockSizeY >= 1 || gpuBlockSizeZ >= 1)

      gpuBlockSizeList(idx)(0) = gpuBlockSizeX
      gpuGridSizeList(idx)(0) = gpuGridSizeX
      gpuBlockSizeList(idx)(1) = gpuBlockSizeY
      gpuGridSizeList(idx)(1) = gpuGridSizeY
      gpuBlockSizeList(idx)(2) = gpuBlockSizeZ
      gpuGridSizeList(idx)(2) = gpuGridSizeZ
      val tmpGpuSharedMemory : Int = localCF.gpuParams match {
        case Some(gpuShCompute) => gpuShCompute.sharedMemorySize.getOrElse(0)
        case None => 0
      }
      gpuSharedMemory = tmpGpuSharedMemory
    })

    (stagesCount, gpuGridSizeList, gpuBlockSizeList, gpuSharedMemory)
  }

  def createAllInputVariables(inputSchema : StructType,
                              ctx : CodegenContext): Array[Variable] = {
    val variables = ArrayBuffer.empty[Variable]
    (0 until inputSchema.length).map(inIdx => {
      val fieldname = inputSchema(inIdx).name
      variables += Variable("in_"+fieldname,
        GPUINPUT | RDDOUTPUT,
        inputSchema(inIdx).dataType,
        inIdx,
        inIdx,
        1,  // For Array input Variables, length will be determined at runtime.
        0, // Will be applicable for GPUOUTPUT
        ctx
      )
    })
    variables.toArray
  }

  def generate(inputSchema : StructType, outputSchema : StructType,
                   cf : DSCUDAFunction, args: Array[Any],
               outputArraySizes: Array[Int]) : JCUDACodegenIterator = {

    val ctx = new CodegenContext()


    val variables = if (cf.funcName != "")
      createVariables(inputSchema,outputSchema,cf,args,
       outputArraySizes, ctx)
    else
      createAllInputVariables(inputSchema, ctx)

    val debugMode = SparkEnv.get.conf.getInt("spark.gpuenabler.DebugMode", 0)
    if(debugMode == 2)
      println("Compile Existing File - DebugMode")

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
        |import jcuda.runtime.cudaDeviceProp;
        |import org.apache.spark.sql.catalyst.InternalRow;
        |import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
        |import com.ibm.gpuenabler.JCUDACodegenIterator;
        |import org.apache.spark.unsafe.types.UTF8String;
        |import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
        |import org.apache.spark.sql.Encoder;
        |import org.apache.spark.sql.types.StructType;
         |import java.lang.reflect.Method;
        |
        |import java.nio.*;
        |import java.util.Iterator;
        |import static jcuda.driver.JCudaDriver.*;
        |import com.ibm.gpuenabler.GPUSparkEnv;
        |import java.util.Map;
        |import java.util.List;
        |import com.ibm.gpuenabler.CachedGPUMeta;
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
        |    private boolean own = true;
        |    private int colWidth = 1;
        |
        |    // cached : 0 - NoCache;
        |    // 1 - DS is cached; Hold GPU results in GPU
        |    // 2 - child DS is cached; Use GPU results stored by child for input parameters
        |    private int cached = 0;
        |    private int blockID = 0;
        |    private List<Map<String,CachedGPUMeta>> gpuPtrs;
        |    private Map<String, CachedGPUMeta> inputCMap;
        |    private Map<String, CachedGPUMeta> outputCMap;
        |    private Encoder<T> inpEnc;
        |
        |    private int[][] blockSizeX;
        |    private int[][] gridSizeX;
        |    private int stages;
        |    private int sharedMemory = 0;
        |
        |    ${getStmt(variables,List("declareHost","declareDevice"),"\n")}
        |    
        |    protected void finalize() throws Throwable
        |    {
        |       super.finalize();
        |       if(freeMemory) {
        |         freePinnedMemory();
        |         freeMemory=false;
        |       }
        |    }    
        |  
        |    public JCUDAIteratorImpl() {
        |        result = new UnsafeRow(${getAttributes(outputSchema).length});
        |        this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
        |        this.rowWriter =
        |           new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 
        |             ${getAttributes(outputSchema).length});
        |        arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
        |    }
        |
        |    public <T> void init(Iterator<InternalRow> inp, Object inprefs[], 
        |           int size, int cached, List<Map<String,CachedGPUMeta>> gpuPtrs,
        |           int blockID, int[][] userGridSizes, int[][] userBlockSizes, int stages, int smSize,
        |           Encoder<T> inpEnc) {
        |        inpitr = inp;
        |        numElements = size;
        |        if (!((cached & 4) > 0) && ${if (cf.funcName != "") true else false }) 
        |          hasNextLoop = ${cf.outputSize.getOrElse("numElements")};
        |        else hasNextLoop = 0;
        |
        |        refs = inprefs;
        |
        |        this.inpEnc = inpEnc;
        |        this.cached = cached;
        |        this.gpuPtrs = gpuPtrs;
        |        this.blockID = blockID;
        |
        |        gridSizeX = userGridSizes;
        |        blockSizeX = userBlockSizes;
        |        this.stages = stages;
        |        this.sharedMemory = smSize;
        |        /* Comparing sharedMemorySize if passed to the max Device shared memory & assert */
        |        cudaDeviceProp deviceProp = new cudaDeviceProp();
        |        JCuda.cudaGetDeviceProperties(deviceProp, 0);
        |        assert ((deviceProp.sharedMemPerBlock) > smSize) : "Invalid shared Memory Size Provided";
        |
        |        if (((cached & 1) > 0)) {
        |           outputCMap = (Map<String, CachedGPUMeta>)gpuPtrs.get(0); 
        |        }
        |
        |        if (((cached & 2) > 0)) {
        |           inputCMap = (Map<String, CachedGPUMeta>) gpuPtrs.get(1);
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
	|       inpitr.hasNext();
         |    ${if (cf.funcName != "" && cf.resource.asInstanceOf[String].endsWith("ptx")) {
                  s"""CUmodule module = GPUSparkEnv.get().cudaManager().getModule("${cf.resource}");"""
                } else {
                  "GPUSparkEnv.get().cudaManager();"
                 }}
        |       ${ if (cf.stagesCount.isEmpty) {
                 s"""| blockSizeX = new int[1][1];
                     | gridSizeX = new int[1][1];
                     |
                     | CUdevice dev = new CUdevice();
                     | JCudaDriver.cuCtxGetDevice(dev);
                     | int dim[] = new int[1];
                     | JCudaDriver.cuDeviceGetAttribute(dim,
                     |   CUdevice_attribute.CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_X, dev);
                     | blockSizeX[0][0] = dim[0];
                     | gridSizeX[0][0] = (int) Math.ceil((double) numElements / blockSizeX[0][0]);
                """.stripMargin
                } else "" }
         |       ${if (cf.funcName != "" && cf.resource.asInstanceOf[String].endsWith("ptx")) {
		 s"""
		   |// Obtain a function pointer to the ${cf.funcName} function.
		   |    CUfunction function = new CUfunction();
		   |    cuModuleGetFunction(function, module, "${cf.funcName}");
		 """.stripMargin
	        } else ""}
        |       cudaStream_t stream = new cudaStream_t();
        |       JCuda.cudaStreamCreateWithFlags(stream, JCuda.cudaStreamNonBlocking);
        |       CUstream cuStream = new CUstream();
        |       JCudaDriver.cuStreamCreate(cuStream, 0);
        |
	|       Boolean enterLoop = true; 
	|       if (!((cached & 2) > 0) ${getStmt(variables,List("checkLoop"),"")} ) {
	|         enterLoop = true;
	|       } else {
	|         enterLoop = false;
	|         allocateMemory(null, cuStream);
	|       } 
	|   
	|       if (enterLoop){
        |         // Fill GPUInput/Direct Copy Host variables
        |         long now = System.nanoTime();
        |         
        |         ExpressionEncoder<T> inExpr = (ExpressionEncoder<T>)inpEnc;
        |         StructType inputSchema = inpEnc.schema();         
        |         
        |         for(int i=0; inpitr.hasNext();i++) {
        |            Object obj = ((InternalRow)inpitr.next()).get(0, inputSchema);
        |            InternalRow r = ((InternalRow) inExpr.toRow(obj));
        |            if (i == 0)  allocateMemory(r, cuStream);
        |            ${getStmt(variables,List("readFromInternalRow"),"")}
        |         }
        |
	|       }
        |  
        |       ${getStmt(variables,List("readFromConstArray"),"")}
        |  
        |       // Flip buffer for read
        |       ${getStmt(variables,List("flip"),"")}
        |  
        |       // Copy data from Host to Device
        |       ${getStmt(variables,List("memcpyH2D"),"")}
        |       cuCtxSynchronize();
        |
         | ${ if (cf.stagesCount.isEmpty) {
          if (cf.funcName != "" && cf.resource.asInstanceOf[String].endsWith("ptx")) {
              s"""
                 |  Pointer kernelParameters = Pointer.to(
                 |    Pointer.to(new int[]{numElements})
                 |    ${getStmt(variables, List("kernel-param"), "")}
                 |  );
                 |  // Call the kernel function.
                 |  cuLaunchKernel(function,
                 |    gridSizeX[0][0], 1, 1,      // Grid dimension
                 |    blockSizeX[0][0], 1, 1,      // Block dimension
                 |    sharedMemory, cuStream,               // Shared memory size and stream
                 |    kernelParameters, null // Kernel- and extra parameters
                 |  );
               """.stripMargin
          } else if (cf.funcName != "") { // External Module Invocation
            s"""
               |  Object[] kernelParameters = {
               |    numElements
               |    ${getStmt(variables, List("kernel-param-mod"), "")}
               |  };
               |  String methodName = "${cf.funcName}";
               |  String className = "${cf.resource}";
               |  System.out.println("Invoke method: " + methodName + " in class " + className);
               |
               |  // Call the kernel function.
               |  try {
               |    Class classRef = Class.forName(className);
               |    Object instance = classRef.newInstance();
               |    try {
               |       Class[] paramObj = new Class[1];
               |       paramObj[0] = Object[].class;
               |
               |       Method method = classRef.getDeclaredMethod(methodName, paramObj);
               |       method.invoke(instance, new Object[]{ kernelParameters });
               |    } catch (Exception ex) {
               |      ex.printStackTrace();
               |    }
               |  } catch (Exception ex) {
               |      ex.printStackTrace();
               |  }
               """.stripMargin
              } else ""
            } else
              if (cf.funcName != "") {
              s"""
                 | for (int stage = 0; stage < stages; stage++) {
                 |    Pointer kernelParameters = Pointer.to(
                 |      Pointer.to(new int[]{numElements})
                 |      ${getStmt(variables, List("kernel-param"), "")}
                 |      ,Pointer.to(new int[]{stage})   // Stage number
                 |      ,Pointer.to(new int[]{stages})   // Total Stages
                 |    );
                 |    // Call the kernel function.
                 |    cuLaunchKernel(function,
                 |      gridSizeX[stage][0],gridSizeX[stage][1], gridSizeX[stage][2],      // Grid dimension
                 |      blockSizeX[stage][0], blockSizeX[stage][1], blockSizeX[stage][2],      // Block dimension
                 |      sharedMemory, cuStream,               // Shared memory size and stream
                 |      kernelParameters, null // Kernel- and extra parameters
                 |    );
                 |
                 | }
                 |
               """.stripMargin
            } else ""
        }
        |        ${if (cf.funcName != "") {
                   // If no kernel is executed as in case of loadGpu; no need to copy data back.        
			getStmt(variables, List("memcpyD2H"), "")
                 } else ""}        
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

    val fpath = if (cf.funcName != "") s"/tmp/GeneratedCode_${cf.funcName}.java"
		else "/tmp/GeneratedCode_autoload.java"

    if(debugMode == 2) {
      println(s"Compile Existing File - ${fpath}")
      val _codeBody = generateFromFile(fpath).filter(!_.contains("REMOVE")).map(x => x+"\n").mkString
      val code = new CodeAndComment(_codeBody, ctx.getPlaceHolderToComments())

      CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDACodegenIterator]
    } else {
      if (debugMode == 1) {
        val code = new CodeAndComment(codeBody,ctx.getPlaceHolderToComments())
        writeToFile(code, fpath)
      }
      val code = codeBody.split("\n").filter(!_.contains("REMOVE")).map(_ + "\n").mkString
      val p = cache.get(new CodeAndComment(code, ctx.getPlaceHolderToComments())).
        generate(ctx.references.toArray).asInstanceOf[JCUDACodegenIterator]
      p
    }
  }

  private val cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[CodeAndComment, GeneratedClass]() {
        override def load(code: CodeAndComment): GeneratedClass = {
          val result = CodeGenerator.compile(code)
          result
        }
      })

  def writeToFile(code: CodeAndComment, fpath: String): Unit = {
    println("The generated file path = " + fpath)
    val pw = new PrintWriter(new File(fpath))
    pw.write(CodeFormatter.format(code))
    pw.close()
  }

  def generateFromFile(fpath : String) : Iterator[String] = {
    scala.io.Source.fromFile(fpath).getLines() 
  }
}


