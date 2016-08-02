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

  def printStaticImports() = {
    val code =
      """
        |        import jcuda.Pointer;
        |        import jcuda.Sizeof;
        |        import jcuda.driver.CUdeviceptr;
        |        import jcuda.driver.CUfunction;
        |        import jcuda.driver.CUmodule;
        |        import org.apache.spark.sql.catalyst.InternalRow;
        |        import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
        |        import org.apache.spark.sql.gpuenabler.JCUDAInterface;
        |        import org.apache.spark.unsafe.types.UTF8String;
        |
        |        import java.nio.CharBuffer;
        |        import java.nio.IntBuffer;
        |        import java.util.Iterator;
        |        import static jcuda.driver.JCudaDriver.*;
        |        import com.ibm.gpuenabler.GPUSparkEnv;
      """.stripMargin
    code
  }

  def printStaticConstructors() = {
    val code =
      """
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
      """.stripMargin
    code
  }

  def printStaticVariables() = {
    val codeBody = new StringBuilder

    codeBody ++=
      """
        |        //Static variables
        |        private Object[] references;
        |        private UnsafeRow result;
        |        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
        |        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
        |        private Iterator<InternalRow> inpitr = null;
        |        private boolean processed = false;
        |        private int idx = 0;
        |        private int numElements = 0;
      """.stripMargin



    codeBody.toString
  }

  def generate(inputSchema: StructType): JCUDAInterface = {

    val ctx = newCodeGenContext()

    val codeBody =  new StringBuilder
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody.toString(), ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]
    return p;
  }

  def generateTest(inputSchema : StructType, outputSchema : StructType,
                   cf : CudaFunc, maxRows : Int) : JCUDAInterface = {

    case class variable(name:String, hostVarName:String, devVarName : String, dtype: String, pos : Int)

    def dtypeResolve(dtype : String) = if(dtype.equalsIgnoreCase("String")) "UTF8String" else dtype

    def findSchemaIndex(schema : StructType, name : String) = {
      val pos = schema.toAttributes.indexWhere(a => a.name.equalsIgnoreCase(name))
      pos
    }

    println("Input Args in CF"+cf.inputArgs.toList)
    cf.inputArgs.foreach(x=>println(x.name))

    var gpu_inputVariables = cf.inputArgs.map {
      x =>
        variable(x.name,
          "hostinput_"+x.name,
          "deviceinput_"+x.name,
           dtypeResolve(x.dtype),
          findSchemaIndex(inputSchema,x.name)
        )
    }.toSeq

    val gpu_outputVariables = cf.outputArgs.map {
      x => variable(x.name,
        "hostoutput_"+x.name,
        "deviceoutput_"+x.name,
        dtypeResolve(x.dtype), -1)
    }.toSeq


    def findVariableName(name : String) = {
      val v = gpu_outputVariables.find(x => x.name.equals(name)).getOrElse {
        gpu_inputVariables.find(x=> x.name.equals(name)).getOrElse {
          val s = inputSchema.toAttributes.find(x => x.name.equals(name)).get
          val v = variable(s.name,
                          "hostinput_"+s.name,null,
                          dtypeResolve(s.dataType.typeName),
                          findSchemaIndex(inputSchema,name))
          gpu_inputVariables = gpu_inputVariables :+ v
          v
        }
      }
      v.hostVarName
    }

    val output_schemaVariables = outputSchema.toAttributes.map {
      x =>
        println("finding schema index for " + x.name)
        variable(x.name,
          findVariableName(x.name),
          null,
          dtypeResolve(x.dataType.typeName),
          findSchemaIndex(outputSchema, x.name))
    }

    output_schemaVariables.foreach(x => println(s"outschema ${x.name} , ${x.hostVarName} ${x.dtype} ${x.pos}"))


    def printDynamicVariables() = {
      val codeBody =  new StringBuilder
      codeBody.append("\n //host input variables\n")
      gpu_inputVariables.foreach(x => codeBody.append(s"private ${x.dtype} ${x.hostVarName}[] = new ${x.dtype}[$maxRows];\n"))
      codeBody.append("\n //host output variables\n")
      gpu_outputVariables.foreach(x => codeBody.append(s"private ${x.dtype} ${x.hostVarName}[] = new ${x.dtype}[$maxRows];\n"))
      codeBody.toString()
    }

    def printInterfaceStaticFunctions() = {
      val cnt = outputSchema.toAttributes.length
      s"""
         |    public myJCUDAInterface() {
         |        result = new UnsafeRow($cnt);
         |        this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
         |        this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, $cnt);
         |    }
         |
         |    public void init(Iterator<InternalRow> inp) {
         |        inpitr = inp;
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
       """.stripMargin
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

      output_schemaVariables.foreach(x => {
        codeBody.append(s"rowWriter.write(${x.pos},${x.hostVarName}[idx]);\n")
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

      def funcName(name : String,pos : Int) = {
        if(name.endsWith("String"))
          s"get$name($pos).clone()"
        else
          "get" + name(0).toUpper + name.substring(1) + s"($pos)"
      }

      def printExtractFromRow = {
        val codeBody =  new StringBuilder

        gpu_inputVariables.foreach( x => {
         codeBody.append{
           s"${x.hostVarName}[i] = r.${funcName(x.dtype,x.pos)}; \n"
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
          if(x.devVarName != null)
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
               |     cuMemcpyDtoH(Pointer.to(${x.hostVarName}),${x.devVarName},numElements * Sizeof.${x.dtype.toUpperCase});
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
              |     cuMemAlloc(${x.devVarName} , numElements * Sizeof.${x.dtype.toUpperCase});
              |     cuMemcpyHtoD(${x.devVarName}, Pointer.to(${x.hostVarName}),numElements * Sizeof.${x.dtype.toUpperCase});
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
                 |     cuMemAlloc(${x.devVarName} , numElements * Sizeof.${x.dtype.toUpperCase});
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
           |    for(int i=0; inpitr.hasNext();i++) {
           |        InternalRow r = (InternalRow) inpitr.next();
           |        $printExtractFromRow
           |        numElements++;
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


    def printInterfaceBody() = {
      s"""
       |class myJCUDAInterface extends JCUDAInterface {
       |$printStaticVariables
       |$printDynamicVariables
       |$printInterfaceStaticFunctions
       |$printNext
       |$printProcessGPU
       |}
      """.
        stripMargin
      }


    val ctx = newCodeGenContext()

    val codeBody =  new StringBuilder

    codeBody ++=
      s"""
        |${printStaticImports()}
        |public class JCUDAVecAdd { // REMOVE
        |$printStaticConstructors
        |$printInterfaceBody
        |} // REMOVE
      """.stripMargin

    val codeString = codeBody.toString().split("\n").filter(x=> (!(x.contains("REMOVE")))).map(x => x+"\n").mkString
    val code = new CodeAndComment(codeString,ctx.getPlaceHolderToComments())

    val formatedCode = CodeFormatter.format(new CodeAndComment(codeBody.toString(),ctx.getPlaceHolderToComments()))
    import java.io._
    val pw = new PrintWriter(new File("/tmp/gpuTest.java" ))
    pw.write(formatedCode);
    pw.close

    println(CodeFormatter.format(code))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]

    return p;

  }

  def generateFromFile(inputSchema: StructType): JCUDAInterface = {
    val ctx = newCodeGenContext()


    val c = scala.io.Source.fromFile("/Users/madhusudanan/spark-projects/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    //val c = scala.io.Source.fromFile("/home/kmadhu/GPUEnabler/gpu-enabler/src/main/scala/org/apache/spark/sql/gpuenabler/JCUDAVecAdd.java").getLines()
    val codeBody = c.filter(x=> (!(x.contains("REMOVE")))).map(x => x+"\n").mkString

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val p = CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[JCUDAInterface]
    return p;
  }

}

