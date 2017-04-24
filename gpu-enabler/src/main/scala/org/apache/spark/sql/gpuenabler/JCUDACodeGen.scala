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
import org.apache.spark.sql.types._
import jcuda.driver.{CUdeviceptr, CUfunction, CUstream}
import java.io.{File, PrintWriter}

import jcuda.Pointer
import org.apache.spark.SparkEnv
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Interface for generated predicate
  */
abstract class JCUDAInterface {
  def hasNext() : Boolean
  def next() : InternalRow
  def init(itr : java.util.Iterator[InternalRow], args: Array[AnyRef],size : Int)
}

/**
  * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[InternalRow]].
  */
object JCUDACodeGen extends Logging {

  val GPUINPUT  = 1
  val GPUOUTPUT = 2
  val RDDOUTPUT = 4
  val CONST     = 8

  case class Variable(colName:String,
                      varType : Int,
                      dataType: DataType,
                      inSchemaIdx : Int,
                      outSchemaIdx : Int,
                      length : Long,
                      outputSize: Long,
                      ctx : CodegenContext) {

    //TODO use case class

    if (is(GPUOUTPUT))
      if (is(GPUINPUT)) assume(false, "A column cannot be in both GPUINPUT & GPUOUTPUT");

    if (is(GPUOUTPUT))
      if (!is(RDDOUTPUT)) assume(false, "GPU OUTPUT column must be RDDOUT to get type details")

    val codeStmt = scala.collection.mutable.Map.empty[String, String]

    def is(t: Int) = ((varType & t) > 0)



    var boxType = ctx.boxedType(dataType);
    var javaType = ctx.javaType(dataType);
    var isArray = false;
    var arrayType: DataType = _;
    var size = ""
    var hostVariableName = ""
    var deviceVariableName = ""

    varType match {
      case _ if is(GPUINPUT) || is(CONST) => {
        hostVariableName = s"gpuInputHost_$colName"
        deviceVariableName = s"gpuInputDevice_$colName"
      }
      case _ if is(GPUOUTPUT) => {
        hostVariableName = s"gpuOutputHost_$colName"
        deviceVariableName = s"gpuOutputDevice_$colName"
      }
      case _ if is(RDDOUTPUT) => {
        hostVariableName = s"directCopyHost_$colName"
      }
    }

    if(is(CONST))
      size = s"$length * Sizeof.${javaType.toUpperCase()}"
    else if (outputSize != 0 && is(GPUOUTPUT))
      size = s"${outputSize} * Sizeof.${javaType.toUpperCase()}"
    else
      size = s"numElements * Sizeof.${javaType.toUpperCase()}"

    dataType match {
      case ArrayType(d, _) =>
        isArray = true;
        arrayType = d;
        boxType = ctx.boxedType(d)
        javaType = ctx.javaType(d)
        if (outputSize != 0 && is(GPUOUTPUT))
          size = s"${outputSize} * Sizeof.${javaType.toUpperCase()} * ${hostVariableName}_numCols"
        else
          size = s"numElements * Sizeof.${javaType.toUpperCase()} * ${hostVariableName}_numCols"
      case _ =>
    }

    if(boxType.eq("Integer")) boxType = "Int"

    codeStmt += "declareHost" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || is(CONST)) {
        s"""
         |private ByteBuffer ${hostVariableName};
         |private MemoryPointer pinMemPtr_$colName;
         |${if(isArray) s"private int ${hostVariableName}_numCols;" else ""}
           """.stripMargin
      }
      else if (is(RDDOUTPUT)){
        if (isArray)
          s"""
            |private $javaType ${hostVariableName}[][];
            |private int ${hostVariableName}_numCols;
           """.stripMargin
        else
          s"private $javaType ${hostVariableName}[];\n"
      }
      else ""
    }

    codeStmt += "declareDevice" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        s"private MemoryPointer ${deviceVariableName};\n"
      else
        ""
    }

    codeStmt += "allocateHost" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        s"""|pinMemPtr_$colName = new MemoryPointer();
            |${
               if (isArray) {
                 if(is(GPUINPUT))
                 s"${hostVariableName}_numCols = r.getArray($inSchemaIdx).numElements();"
                 else
                 s"${hostVariableName}_numCols = $length;"
               }
               else ""
            }
            |cuMemAllocHost(pinMemPtr_$colName, $size );
            |$hostVariableName = pinMemPtr_$colName.getByteBuffer(0,$size).
            |                  order(ByteOrder.LITTLE_ENDIAN);
           """.stripMargin
      else if(is(RDDOUTPUT)) {
        if (isArray)
          s"""
           |${hostVariableName}_numCols = r.getArray($inSchemaIdx).numElements();
           |$hostVariableName = new $javaType[numElements][${hostVariableName}_numCols];
           |""".stripMargin
        else
          s"$hostVariableName = new $javaType[numElements];\n"
      }
      else
        ""
    }

    codeStmt += "allocateDevice" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        s"""|$deviceVariableName = new MemoryPointer();
            |cuMemAlloc($deviceVariableName, $size);
           """.stripMargin
      else
       ""
    }

    codeStmt += "readFromInternalRow" -> {
      if (is(GPUINPUT)) {
        if (isArray)
          s"""
           |$javaType tmp_${colName}[] = r.getArray($inSchemaIdx).to${boxType}Array();
           |for(int j = 0; j < gpuInputHost_${colName}_numCols; j ++)
           |  ${hostVariableName}.put$boxType(tmp_${colName}[j]);
        """.stripMargin
        else
          s"${hostVariableName}.put$boxType(${ctx.getValue("r", dataType, inSchemaIdx.toString)});\n"
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
      if(is(CONST)) {
        s"""
         | for(int i = 0; i<$length; i++ ) {
         |  $hostVariableName.put$boxType((($javaType[])refs[$colName])[i]);
         | }
       """.stripMargin
        }
      else
        ""
    }


    codeStmt += "flip" -> {
      if(is(GPUINPUT) || is(CONST))
        s"${hostVariableName}.flip();\n"
      else
        ""
    }

    codeStmt += "memcpyH2D" ->{
      if(is(GPUINPUT) || is(CONST))
        s"""|  cuMemcpyHtoD($deviceVariableName,
            |      Pointer.to($hostVariableName),
            |      $size);
        """.stripMargin
      else
        ""
    }

    codeStmt += "kernel-param" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        s",Pointer.to($deviceVariableName)\n"
      else
        ""
    }

    codeStmt += "memcpyD2H" -> {
      if(is(GPUOUTPUT))
        s"cuMemcpyDtoH(Pointer.to(${hostVariableName}), $deviceVariableName, $size);\n"
      else
        ""
    }

    // Device memory will be freed immediatly.
    codeStmt += "FreeDeviceMemory" -> {
      if(is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        s"""|cuMemFree($deviceVariableName);
         """.stripMargin
      else
        ""
    }

    //Host Memory will be freed only after iterator completes.
    codeStmt += "FreeHostMemory" -> {
      if (is(GPUINPUT) || is(GPUOUTPUT) || is(CONST))
        s"cuMemFreeHost(pinMemPtr_$colName);\n"
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
                      cf : DSCUDAFunction, args : Array[AnyRef],
                      outputArraySizes: Seq[Int], ctx : CodegenContext) = {
    // columns to be copied from inputRow to outputRow without gpu computation.
    val variables = ArrayBuffer.empty[Variable]

    def findSchemaIndex(schema: StructType, colName: String) =
      schema.toAttributes.indexWhere(a => a.name.equalsIgnoreCase(colName))

    cf._inputColumnsOrder.foreach {
      x => {
        val inIdx = findSchemaIndex(inputSchema, x)
        assume(inIdx >= 0, s"$inIdx ${x} not available in input Schema")
        val outIdx = findSchemaIndex(outputSchema, x)
        variables += Variable(x,
          GPUINPUT | {
            if (outIdx > 1) RDDOUTPUT else 0
          },
          inputSchema(inIdx).dataType,
          inIdx,
          outIdx,
          1, cf.outputSize.getOrElse(0),
          // x.length.toLong,
          ctx
        )
      }
    }

    var cnt = 0
    args.foreach {
      x => {
        val dtype =
          x match {
            case y: Array[Long] => {
              (LongType, y.length)
            }
            case y: Array[Int] => {
              (IntegerType, y.length)
            }
            case y: Array[Char] => {
              (ByteType, y.length)
            }
          }

        variables += Variable(cnt.toString(),
          CONST,
          dtype._1,
          -1,
          -1,
          dtype._2, cf.outputSize.getOrElse(0),
          ctx
        )
        cnt += 1
      }
    }

    if (outputArraySizes == null) {
      cf._outputColumnsOrder.foreach {
        x => {
          val outIdx = findSchemaIndex(outputSchema, x)

          // GPU OUTPUT variables must be in the output -- TODO may need to relax
          assume(outIdx >= 0)

          variables += Variable(x,
            GPUOUTPUT | RDDOUTPUT,
            outputSchema(outIdx).dataType,
            -1,
            outIdx,
            1, //x.length.toLong, JOE - how to get partition count
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

        variables += Variable(col._1,
          GPUOUTPUT | RDDOUTPUT,
          outputSchema(outIdx).dataType,
          -1,
          outIdx,
          col._2, //x.length.toLong, JOE - how to get partition count
          cf.outputSize.getOrElse(0),
          ctx)
      })
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
            -1, cf.outputSize.getOrElse(0),
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
                   cf : DSCUDAFunction, args: Array[AnyRef],
               outputArraySizes: Seq[Int], cached: Int,
               gpuPtrs: Array[mutable.HashMap[String, CUdeviceptr]]) : JCUDAInterface = {

    val ctx = new CodegenContext()

    val variables = createVariables(inputSchema,outputSchema,cf,args,outputArraySizes, ctx)
    val debugMode = !SparkEnv.get.conf.get("DebugMode","").isEmpty
    if(debugMode)
      println("Compile Existing File - DebugMode");
    else
      println("Generate Code")

    println(s"Cached $cached for ${cf.funcName}")
    if (cached == 1) {
      if (gpuPtrs(0) != null) println("Output holder ready to be written")
      else println("ERROR for output holder")

    }

    if (cached == 2) {
      if (gpuPtrs(1) != null) println("Input holder ready to be read")
      else print("ERROR for input holder")
    }

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
        |public class GeneratedCode_${cf.funcName} { // REMOVE
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
        |    class MemoryPointer extends CUdeviceptr {
        |       MemoryPointer() {
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
        |        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter;
        |        private Iterator<InternalRow> inpitr = null;
        |        private boolean processed = false;
        |        private boolean freeMemory = true;
        |        private int idx = 0;
        |        private int numElements = 0; 
        |        private int hasNextLoop = 0;
        |        private Object refs[];
        |
        |        ${getStmt(variables,List("declareHost","declareDevice"),"\n")}
        |
        |    public myJCUDAInterface() {
        |        result = new UnsafeRow(${outputSchema.toAttributes.length});
        |        this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 64);
        |        this.rowWriter =
        |           new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, ${outputSchema.toAttributes.length});
        |        arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
        |    }
        |
        |    public void init(Iterator<InternalRow> inp, Object inprefs[], int size) {
        |        inpitr = inp;
        |        numElements = size;
        |        hasNextLoop = ${ cf.outputSize match {
        case Some(outsize) => outsize
        case None => "numElements"
      } };
        |        refs = inprefs;
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
        |    private void allocateMemory(InternalRow r) {
        |
        |       // Allocate Host and Device variables
        |       ${getStmt(variables,List("allocateHost","allocateDevice"),"\n")}
        |    }
        |
        |    public void processGPU() {
        |       inpitr.hasNext();
        |       CUmodule module = GPUSparkEnv.get().cudaManager().getModule("${cf.resource}");
        |
        |       // Obtain a function pointer to the ${cf.funcName} function.
        |       CUfunction function = new CUfunction();
        |       cuModuleGetFunction(function, module, "${cf.funcName}");
        |
        |       // Fill GPUInput/Direct Copy Host variables
        |       for(int i=0; inpitr.hasNext();i++) {
        |          InternalRow r = (InternalRow) inpitr.next();
        |          if (i == 0)  allocateMemory(r);
        |          ${getStmt(variables,List("readFromInternalRow"),"")}
        |       }
        |
        |      ${getStmt(variables,List("readFromConstArray"),"")}
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
        |        ${getStmt(variables,List("FreeDeviceMemory"),"")}
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

    val fpath = s"${Utils.homeDir}GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/GeneratedCode_${cf.funcName}.java"

    def writeToFile(codeBody : String): Unit = {
      val code = new CodeAndComment(codeBody,ctx.getPlaceHolderToComments())
      val pw = new PrintWriter(new File(fpath))
      pw.write(CodeFormatter.format(code))
      pw.close
      // println(CodeFormatter.format(code))
      println("The generated file path = " + fpath)

    }

    if(debugMode)
      return generateFromFile(fpath)
    else {
      writeToFile(codeBody)

      val code = codeBody.split("\n").filter(!_.contains("REMOVE")).map(_ + "\n").mkString
      val p = CodeGenerator.compile(new CodeAndComment(code, ctx.getPlaceHolderToComments())).
        generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]

      if (Utils.homeDir.contains("madhusudanan"))
        return new JCUDAVecAdd().generate()
      else {
        return p;
      }
    }
  }

  def generateFromFile(fpath : String) : JCUDAInterface = {

    val ctx = new CodegenContext()

    val c = scala.io.Source.fromFile(fpath).getLines()
    val codeBody = c.filter(x=> (!(x.contains("REMOVE")))).map(x => x+"\n").mkString

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]
    return p;
  }

}


