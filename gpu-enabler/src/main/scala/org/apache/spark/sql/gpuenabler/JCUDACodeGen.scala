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
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{DataType, StringType, StructType}

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

  case class Variable(colName:String, hostVarName:String, devVarName : String, dtype: DataType, schemaIdx : Int)

  def findSchemaIndex(schema : StructType, colName : String) =
     schema.toAttributes.indexWhere(a => a.name.equalsIgnoreCase(colName))

  def generate(inputSchema : StructType, outputSchema : StructType,
                   cf : CudaFunc) : JCUDAInterface = {

    val ctx = newCodeGenContext()

    val gpu_inputVariables = cf.inputArgs.map {
      x => {
        val idx = findSchemaIndex(inputSchema, x.name)
        Variable(x.name,
          "hostinput_" + x.name,
          "deviceinput_" + x.name,
          inputSchema(idx).dataType,
          idx
        )
      }
    }.toSeq

    val gpu_outputVariables = cf.outputArgs.map {
      x => {
        val idx = findSchemaIndex(outputSchema, x.name)
        Variable(x.name,
          "hostoutput_" + x.name,
          "deviceoutput_" + x.name,
          outputSchema(idx).dataType, -1)
      }
    }.toSeq


    // columns to be copied from inputRow to outputRow without gpu computation.
    var directCopyVariables : Seq[Variable]  = Seq.empty[Variable]

    // TO write to output internal Row
    val output_Variables = outputSchema.toAttributes.map {
      x =>
        def findVariableName(colName : String) = {
          val v = gpu_outputVariables.find(x => x.colName.equals(colName)).getOrElse {
            gpu_inputVariables.find(x=> x.colName.equals(colName)).getOrElse {
              val s = inputSchema.toAttributes.find(x => x.name.equals(colName)).get
              val v = Variable(s.name,
                "hostinput_"+s.name,null,
                s.dataType,
                findSchemaIndex(inputSchema,colName))
              directCopyVariables = directCopyVariables :+ v
              v
            }
          }
          v.hostVarName
        }
        Variable(x.name,
          findVariableName(x.name),
          null,
          x.dataType,
          findSchemaIndex(outputSchema, x.name))
    }


    def printVariables() = {
      val codeBody =  new StringBuilder
      codeBody.append("\n //TODO convert it to ArrayBuffer, also try to copy to pinned mem directly")
      codeBody.append("\n //host input variables\n")
      gpu_inputVariables.foreach(x => codeBody.append(s"private ${ctx.javaType(x.dtype)} ${x.hostVarName}[];\n"))
      codeBody.append("\n //direct copy variables\n")
      directCopyVariables.foreach(x => codeBody.append(s"private ${ctx.javaType(x.dtype)} ${x.hostVarName}[];\n"))
      codeBody.append("\n //host output variables\n")
      gpu_outputVariables.foreach(x => codeBody.append(s"private ${ctx.javaType(x.dtype)} ${x.hostVarName}[];\n"))
      codeBody.toString()
    }

    def printNext() = {
      val codeBody =  new StringBuilder

      codeBody.append {
        """
          |        public InternalRow next() {
          |            holder.reset();
          |            rowWriter.zeroOutNullBytes();
        """.stripMargin
      }

      output_Variables.foreach(x => {
        codeBody.append(s"rowWriter.write(${x.schemaIdx},${x.hostVarName}[idx]);\n")
      })

      codeBody.append {
        """
          |      result.setTotalSize(holder.totalSize());
          |      idx++;
          |      return (InternalRow) result;
          | }
        """.stripMargin
      }
      codeBody.toString()
    }

    def printProcessGPU() = {

      def funcName(name : String,schemaIdx : Int) = {
        if(name.endsWith("String"))
          s"get$name($schemaIdx).clone()"
        else
          "get" + name(0).toUpper + name.substring(1) + s"($schemaIdx)"
      }

      def printAllocateMemory = {
        val codeBody =  new StringBuilder
        codeBody.append("\n //TODO convert it to ArrayBuffer, also try to copy to pinned mem directly")
        codeBody.append("\n //host input variables\n")
        gpu_inputVariables.foreach(x => codeBody.append(s"${x.hostVarName} = new ${ctx.javaType(x.dtype)}[numElements];\n"))
        codeBody.append("\n //direct copy variables\n")
        directCopyVariables.foreach(x => codeBody.append(s"${x.hostVarName} = new ${ctx.javaType(x.dtype)}[numElements];\n"))
        codeBody.append("\n //host output variables\n")
        gpu_outputVariables.foreach(x => codeBody.append(s"${x.hostVarName} = new ${ctx.javaType(x.dtype)}[numElements];\n"))
        codeBody.toString()
      }

      def printExtractFromRow = {
        val codeBody =  new StringBuilder

        (gpu_inputVariables ++ directCopyVariables).foreach( x => {
         codeBody.append{
           x.dtype match {
             case StringType => s"${x.hostVarName}[i] = ${ctx.getValue("r",x.dtype,x.schemaIdx.toString)}.clone(); \n"
             case _ => s"${x.hostVarName}[i] = ${ctx.getValue("r",x.dtype,x.schemaIdx.toString)}; \n"
           }
         }
        })
        codeBody.toString()
      }

      def printCallKernel = {
        var codeBody =  new StringBuilder

        codeBody.append(
          """
            |    // Set up the kernel parameters: A pointer to an array
            |    // of pointers which point to the actual values.
            |        Pointer kernelParameters = Pointer.to(
            |            Pointer.to(new int[]{numElements}),
          """.stripMargin)

        (gpu_inputVariables ++ gpu_outputVariables).foreach( x => {
            codeBody.append(s" Pointer.to(${x.devVarName}),\n")
        })
        codeBody = codeBody.dropRight(2)
        codeBody.append("\n);")

        codeBody.append(
          """
            |
            |
            |    // Call the kernel function.
            |    int blockSizeX = 256;
            |    int gridSizeX = (int) Math.ceil((double) numElements / blockSizeX);
            |
            |    cuLaunchKernel(function,
            |                    gridSizeX, 1, 1,      // Grid dimension
            |                    blockSizeX, 1, 1,      // Block dimension
            |                    0, null,               // Shared memory size and stream
            |                    kernelParameters, null // Kernel- and extra parameters
            |     );
            |
            |     cuCtxSynchronize();
          """.stripMargin
        )

        codeBody.append(
          s"""
             |    // Allocate host output memory and copy the device output
             |    // to the host.
          """.stripMargin)

        (gpu_outputVariables).foreach(x => {
          codeBody.append {
            s"""
               |     cuMemcpyDtoH(Pointer.to(${x.hostVarName}),${x.devVarName},numElements * Sizeof.${ctx.javaType(x.dtype).toUpperCase});
              """
          }
        })

        codeBody.append("\n\n//clean up \n");


        (gpu_inputVariables ++ gpu_outputVariables).foreach( x => {
          if(x.devVarName != null)
            codeBody.append(s"cuMemFree(${x.devVarName}); \n")
        })

        codeBody.toString()
      }


      def printDeviceInputVariables() = {
        val codeBody =  new StringBuilder

        (gpu_inputVariables).foreach(x => {
          if (x.devVarName != null) {
            codeBody.append {
              s"""
              |     CUdeviceptr ${x.devVarName} = new CUdeviceptr();
              |     cuMemAlloc(${x.devVarName} , numElements * Sizeof.${ctx.javaType(x.dtype).toUpperCase});
              |     cuMemcpyHtoD(${x.devVarName}, Pointer.to(${x.hostVarName}),numElements * Sizeof.${ctx.javaType(x.dtype).toUpperCase});
              """
            }
          }
        })

        codeBody.append("\n// Allocate device output memory")

        (gpu_outputVariables).foreach(x => {
          if (x.devVarName != null) {
            codeBody.append {
              s"""
                 |     CUdeviceptr ${x.devVarName} = new CUdeviceptr();
                 |     cuMemAlloc(${x.devVarName} , numElements * Sizeof.${ctx.javaType(x.dtype).toUpperCase});
              """
            }
          }
        })
        codeBody.toString()
      }

      val codeBody =  new StringBuilder

      codeBody.append {
        s"""
           |public void processGPU() {
           | CUmodule module = GPUSparkEnv.get().cudaManager().getModule("${cf.func.ptxPath}");
           |
           |    // Obtain a function pointer to the "add" function.
           |    CUfunction function = new CUfunction();
           |    cuModuleGetFunction(function, module, "${cf.func.fname}");
           |
           |    // Allocate and fill the host hostinput data
           |    $printAllocateMemory
           |    for(int i=0; inpitr.hasNext();i++) {
           |        InternalRow r = (InternalRow) inpitr.next();
           |        $printExtractFromRow
           |    }
           |
           |     // Allocate the device hostinput data, and copy the
           |     // host hostinput data to the device
           |     $printDeviceInputVariables
           |
           |     $printCallKernel
           |
           |
           | }
         """.stripMargin
      }

      codeBody.toString()
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
        |import java.nio.CharBuffer;
        |import java.nio.IntBuffer;
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
        |    public void execute() {
        |      processed = true;
        |      processGPU();
        |    }
        |
        |    public boolean hasNext() {
        |        if(!processed) execute();
        |        return idx < numElements;
        |    }
        |
        |      $printVariables
        |      $printNext
        |      $printProcessGPU
        |   }
        |} // REMOVE
      """.stripMargin

    def writeToFile(codeBody : String): Unit = {
      val code = new CodeAndComment(codeBody,ctx.getPlaceHolderToComments())
      import java.io._
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
      return generateFromFile
    else {
      return p;
    }

  }

  def generateFromFile : JCUDAInterface = {

    val ctx = newCodeGenContext()


    val c = scala.io.Source.fromFile(s"${Utils.homeDir}/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    //val c = scala.io.Source.fromFile("/home/kmadhu/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    val codeBody = c.filter(x=> (!(x.contains("REMOVE")))).map(x => x+"\n").mkString

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]
    return p;
  }

}

