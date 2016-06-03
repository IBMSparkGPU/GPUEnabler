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

import java.io._

import scala.reflect.ClassTag

import org.apache.xbean.asm5.{ClassReader, ClassVisitor, MethodVisitor, Type}
import org.apache.xbean.asm5.Opcodes._
import org.apache.xbean.asm5._


private class mapLambdaExpressionExtractor extends ClassVisitor(ASM5) {
  var isValid = false
  var methodName: String = null
  var exprtype: Int = 0
  var exprop: Int = 0
  var exprconst : Long = 0

  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    val idxmc = name.indexOfSlice("$mc")
    val idxsp = name.indexOfSlice("$sp")
    if (idxmc + (3 + 2) == idxsp) {    // find "$mc??$sp"
      isValid = true
      methodName = name
      new MethodVisitor(ASM5) {
        override def visitIntInsn(op: Int, value: Int) {
          if ((op == SIPUSH) || (op == BIPUSH)) {
            exprconst = value
          } else {
            isValid = false
          }
        }
        override def visitLdcInsn(cst: java.lang.Object) {
          cst match {
            case _: java.lang.Integer =>
              exprconst = cst.asInstanceOf[Int]
            case _: java.lang.Long =>
              exprconst = cst.asInstanceOf[Long]
            case _: java.lang.Float =>
              exprconst = java.lang.Float.floatToIntBits(cst.asInstanceOf[Float])
            case _: java.lang.Double =>
              exprconst = java.lang.Double.doubleToLongBits(cst.asInstanceOf[Double])
            case _ =>
              isValid = false
          }
        }
        override def visitInsn(op: Int) {
          op match {
            case ICONST_M1 => exprconst = -1
            case ICONST_0 | LCONST_0 | FCONST_0 | DCONST_0 => exprconst = 0
            case ICONST_1 | LCONST_1 => exprconst = 1
            case ICONST_2 => exprconst = 2
            case ICONST_3 => exprconst = 3
            case ICONST_4 => exprconst = 4
            case ICONST_5 => exprconst = 5
            case FCONST_1 =>
              exprconst = java.lang.Float.floatToIntBits(1.0f)
            case DCONST_1 =>
              exprconst = java.lang.Double.doubleToLongBits(1.0)
            case FCONST_2 =>
              exprconst = java.lang.Float.floatToIntBits(2.0f)
            case IRETURN | LRETURN | FRETURN | DRETURN => { }
            case IADD | LADD | FADD | DADD => exprop = IADD
            case ISUB | LSUB | FSUB | DSUB => exprop = ISUB
            case IMUL | LMUL | FMUL | DMUL => exprop = IMUL
            case IDIV | LDIV | FDIV | DDIV => exprop = IDIV
            case IREM | LREM => exprop = IREM
            case _ =>
              isValid = false
          }
        }
        override def visitVarInsn(op: Int, idx: Int) = {
          op match {
            case ILOAD | LLOAD | FLOAD | DLOAD => {
              if (idx == 1) {
                exprtype = op
              } else {
                isValid = false
              }
            }
            case _ =>
              isValid = false
          }
        }
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) = {
          isValid = false
        }
        override def visitIincInsn(varidx: Int, increment: Int) = {
          isValid = false
        }
        override def visitJumpInsn(op: Int, label: Label) = {
          isValid = false
        }
        override def visitLookupSwitchInsn(dflt: Label, keys: Array[Int], labels: Array[Label]) = {
          isValid = false
        }
        override def visitMethodInsn(op: Int, owner: String, name: String, desc: String,
                                     itf: Boolean) = {
          isValid = false
        }
        override def visitMultiANewArrayInsn(desc: String, dims: Int) = {
          isValid = false
        }
        override def visitTableSwitchInsn(min: Int, max: Int, dflt: Label, labels: Label*) = {
          isValid = false
        }
      }
    } else {
      new MethodVisitor(ASM5) {}
    }
  }
}

private class reduceLambdaExpressionExtractor extends ClassVisitor(ASM5) {
  var isValid = false
  var exprtype: Int = 0

  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    val idxmc = name.indexOfSlice("$mc")
    val idxsp = name.indexOfSlice("$sp")
    if (idxmc + (3 + 3) == idxsp) {    // find "$mc???$sp"
      isValid = true
      new MethodVisitor(ASM5) {
        override def visitInsn(op: Int) {
          op match {
            case IRETURN | LRETURN | FRETURN | DRETURN => { }
            case IADD | DADD => { }
            case _ =>
              isValid = false
          }
        }
        override def visitVarInsn(op: Int, idx: Int) = {
          op match {
            case ILOAD => {
              if (idx == 1 || idx == 2) {
                exprtype = op
              } else {
                isValid = false
              }
            }
            case DLOAD => {
              if (idx == 1 || idx == 3) {
                exprtype = op
              } else {
                isValid = false
              }
            }
            case _ =>
              isValid = false
          }
        }
        override def visitIntInsn(op: Int, value: Int) {
          isValid = false
        }
        override def visitLdcInsn(cst: java.lang.Object) {
          isValid = false
        }
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) = {
          isValid = false
        }
        override def visitIincInsn(varidx: Int, increment: Int) = {
          isValid = false
        }
        override def visitJumpInsn(op: Int, label: Label) = {
          isValid = false
        }
        override def visitLookupSwitchInsn(dflt: Label, keys: Array[Int], labels: Array[Label]) = {
          isValid = false
        }
        override def visitMethodInsn(op: Int, owner: String, name: String, desc: String,
                                     itf: Boolean) = {
          isValid = false
        }
        override def visitMultiANewArrayInsn(desc: String, dims: Int) = {
          isValid = false
        }
        override def visitTableSwitchInsn(min: Int, max: Int, dflt: Label, labels: Label*) = {
          isValid = false
        }
      }
    } else {
      new MethodVisitor(ASM5) {}
    }
  }
}

object CUDACodeGenerator {
  val isGPUCodeGen = GPUSparkEnv.get.isGPUCodeGenEnabled

  private def copyStream(in: InputStream, out: OutputStream): Unit = {
    try {
      val buf = new Array[Byte](8192)
      var n = 0
      while (n != -1) {
        n = in.read(buf)
        if (n != -1) {
          out.write(buf, 0, n)
        }
      }
    } finally {
      try {
        in.close()
      } finally {
        out.close()
      }
    }
  }

  def classReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    // todo: Fixme - continuing with earlier behavior ...
    if (resourceStream == null) {
      new ClassReader(resourceStream)
    } else {
      val baos = new ByteArrayOutputStream(128)
      copyStream(resourceStream, baos)
      new ClassReader(new ByteArrayInputStream(baos.toByteArray))
    }
  }

  def generateForMap[U: ClassTag, T: ClassTag](f: T => U): Option[CUDAFunction] = {
    if (!isGPUCodeGen) { return None }

    // val declaredFields = f.getClass.getDeclaredFields
    // val declaredMethods = f.getClass.getDeclaredMethods
    // println(" + declared fields: " + declaredFields.size)
    // declaredFields.foreach { f => println("     " + f) }
    // println(" + declared methods: " + declaredMethods.size)
    // declaredMethods.foreach { m => println("     " + m) }

    val e = new mapLambdaExpressionExtractor
    val cls = f.getClass
    classReader(cls).accept(e, 0)
    val fullName =
      "_map_" + (f.getClass.getName + "." + e.methodName).replace(".", "_").replace("$", "_")
    if (!e.isValid) { return None }

    val ptxType = e.exprtype match {
      case ILOAD => "s32"
      case LLOAD => "s64"
      case FLOAD => "f32"
      case DLOAD => "f64"
    }
    val ptxShift = e.exprtype match {
      case ILOAD | FLOAD => 2
      case LLOAD | DLOAD => 3
    }
    var ptxReg = e.exprtype match {
      case ILOAD => "regi32"
      case LLOAD => "regi64"
      case FLOAD => "regf32"
      case DLOAD => "regf64"
    }
    val ptxOpRound = e.exprtype match {
      case FLOAD | DLOAD => "rz."
      case _ => ""
    }
    val ptxOp = e.exprop match {
      case IADD => "add"
      case ISUB => "sub"
      case IMUL =>
        ( if (e.exprtype == ILOAD || e.exprtype == LLOAD) "mul.lo" else "mul" )
      case IDIV => "div"
      case IREM => "rem"
      case 0 => "add"
    }
    val ptxConst = e.exprtype match {
      case ILOAD => e.exprconst.toString
      case LLOAD => e.exprconst.toString
      case FLOAD => "0F" + ("00000000" + e.exprconst.toHexString).takeRight(8)
      case DLOAD => "0D" + ("0000000000000000" + e.exprconst.toHexString).takeRight(16)
    }

    val ptxMap = s"""
.version 4.2
.target sm_30
.address_size 64
.visible .entry $fullName(
        .param .u64 _param_0,
        .param .u64 _param_1,
        .param .u64 _param_2
)
{
        .reg .pred      %p<2>;
        .reg .s32       %r<5>;
        .reg .s64       %rd<12>;
        .reg .s32       regi32;
        .reg .s64       regi64;
        .reg .f32       regf32;
        .reg .f64       regf64;
        ld.param.u64    %rd2, [_param_0];
        ld.param.u64    %rd3, [_param_1];
        ld.param.u64    %rd4, [_param_2];

        cvta.to.global.u64      %rd7, %rd2;
        ld.global.s32   %r2, [%rd7];
        cvt.s64.s32     %rd2, %r2;

        mov.u32         %r1, %tid.x;
        cvt.u64.u32     %rd5, %r1;
        mov.u32         %r2, %ctaid.x;
        mov.u32         %r3, %ntid.x;
        mul.wide.u32    %rd6, %r3, %r2;
        add.s64         %rd1, %rd6, %rd5;
        setp.ge.s64     %p1, %rd1, %rd2;
        @%p1 bra        BB_RET;
        cvta.to.global.u64      %rd7, %rd3;
        shl.b64         %rd8, %rd1, $ptxShift;
        add.s64         %rd9, %rd7, %rd8;
        ld.global.$ptxType   $ptxReg, [%rd9];
        $ptxOp.$ptxOpRound$ptxType  $ptxReg, $ptxReg, $ptxConst;
        cvta.to.global.u64      %rd10, %rd4;
        add.s64         %rd11, %rd10, %rd8;
        st.global.$ptxType   [%rd11], $ptxReg;
BB_RET:
        ret;
}"""

    val cudaFunc = new CUDAFunction(fullName, Array("this"), Array("this"), (fullName, ptxMap))
    Some(cudaFunc)
  }

  val ptxIntReduce = """
.version 4.2
.target sm_30
.address_size 64
.visible .entry _intReduce(
        .param .u64 _intReduce_param_0,
        .param .u64 _intReduce_param_1,
        .param .u64 _intReduce_param_2
)
{
        .reg .pred      %p<4>;
        .reg .b32       %r<39>;
        .reg .b64       %rd<13>;
        ld.param.u64         %rd6, [_intReduce_param_0];
        ld.param.u64         %rd7, [_intReduce_param_1];
        ld.param.u64         %rd8, [_intReduce_param_2];
        cvta.to.global.u64         %rd11, %rd8;
        st.global.u32   [%rd11], 0;

        cvta.to.global.u64      %rd12, %rd6;
        ld.global.s32   %r2, [%rd12];
        cvt.s64.s32     %rd6, %r2;

        mov.u32         %r1, %ntid.x;
        mov.u32         %r9, %ctaid.x;
        mov.u32         %r2, %tid.x;
        mad.lo.s32      %r10, %r1, %r9, %r2;
        cvt.u64.u32     %rd12, %r10;
        mov.u32         %r38, 0;
        setp.ge.s64     %p1, %rd12, %rd6;
        @%p1 bra         BB_3;
        cvta.to.global.u64         %rd2, %rd7;
        mov.u32         %r12, %nctaid.x;
        mul.lo.s32      %r13, %r12, %r1;
        cvt.u64.u32    %rd3, %r13;
        mov.u32         %r38, 0;
BB_2:
        shl.b64         %rd9, %rd12, 2;
        add.s64         %rd10, %rd2, %rd9;
        ld.global.u32   %r14, [%rd10];
        add.s32         %r38, %r14, %r38;
        add.s64         %rd12, %rd3, %rd12;
        setp.lt.s64     %p2, %rd12, %rd6;
        @%p2 bra        BB_2;
BB_3:
        mov.u32         %r17, 16;
        mov.u32         %r34, 31;
        shfl.down.b32 %r15, %r38, %r17, %r34;
        add.s32         %r20, %r15, %r38;
        mov.u32         %r21, 8;
        shfl.down.b32 %r19, %r20, %r21, %r34;
        add.s32         %r24, %r19, %r20;
        mov.u32         %r25, 4;
        shfl.down.b32 %r23, %r24, %r25, %r34;
        add.s32         %r28, %r23, %r24;
        mov.u32         %r29, 2;
        shfl.down.b32 %r27, %r28, %r29, %r34;
        add.s32         %r32, %r27, %r28;
        mov.u32         %r33, 1;
        shfl.down.b32 %r31, %r32, %r33, %r34;
        and.b32         %r35, %r2, 31;
        setp.ne.s32     %p3, %r35, 0;
        @%p3 bra        BB_5;
        cvta.to.global.u64         %rd11, %rd8;
        add.s32         %r36, %r31, %r32;
        atom.global.add.u32         %r37, [%rd11], %r36;
BB_5:
        ret;
}
                     """

  val ptxDoubleReduce = """
.version 4.2
.target sm_30
.address_size 64
.visible .entry _doubleReduce(
        .param .u64 _doubleReduce_param_0,
        .param .u64 _doubleReduce_param_1,
        .param .u64 _doubleReduce_param_2
)
{
        .reg .pred         %p<5>;
        .reg .b32         %r<68>;
        .reg .f64         %fd<21>;
        .reg .b64         %rd<18>;
        ld.param.u64         %rd10, [_doubleReduce_param_0];
        ld.param.u64         %rd11, [_doubleReduce_param_1];
        ld.param.u64         %rd12, [_doubleReduce_param_2];

        cvta.to.global.u64      %rd16, %rd10;
        ld.global.s32   %r2, [%rd16];
        cvt.s64.s32     %rd10, %r2;

        mov.u32         %r1, %ntid.x;
        mov.u32         %r3, %ctaid.x;
        mov.u32         %r2, %tid.x;
        mad.lo.s32         %r4, %r1, %r3, %r2;
        cvt.u64.u32        %rd16, %r4;
        mov.f64         %fd20, 0d0000000000000000;
        setp.ge.s64        %p1, %rd16, %rd10;
        @%p1 bra         BB_3;
        cvta.to.global.u64         %rd2, %rd11;
        mov.u32         %r5, %nctaid.x;
        mul.lo.s32         %r6, %r5, %r1;
        cvt.u64.u32        %rd3, %r6;
        mov.f64         %fd20, 0d0000000000000000;
BB_2:
        shl.b64         %rd13, %rd16, 3;
        add.s64         %rd14, %rd2, %rd13;
        ld.global.f64         %fd7, [%rd14];
        add.f64         %fd20, %fd20, %fd7;
        add.s64         %rd16, %rd3, %rd16;
        setp.lt.s64        %p2, %rd16, %rd10;
        @%p2 bra         BB_2;
BB_3:
//        mov.f64         %fd4, %fd20;
        mov.b64 {%r7,%r8}, %fd20;
        mov.u32         %r15, 16;
        mov.u32         %r64, 31;
        shfl.down.b32 %r9, %r7, %r15, %r64;
        shfl.down.b32 %r13, %r8, %r15, %r64;
        mov.b64 %fd9, {%r9,%r13};
        add.f64         %fd10, %fd20, %fd9;
        mov.b64 {%r19,%r20}, %fd10;
        mov.u32         %r27, 8;
        shfl.down.b32 %r21, %r19, %r27, %r64;
        shfl.down.b32 %r25, %r20, %r27, %r64;
        mov.b64 %fd11, {%r21,%r25};
        add.f64         %fd12, %fd10, %fd11;
        mov.b64 {%r31,%r32}, %fd12;
        mov.u32         %r39, 4;
        shfl.down.b32 %r33, %r31, %r39, %r64;
        shfl.down.b32 %r37, %r32, %r39, %r64;
        mov.b64 %fd13, {%r33,%r37};
        add.f64         %fd14, %fd12, %fd13;
        mov.b64 {%r43,%r44}, %fd14;
        mov.u32         %r51, 2;
        shfl.down.b32 %r45, %r43, %r51, %r64;
        shfl.down.b32 %r49, %r44, %r51, %r64;
        mov.b64 %fd15, {%r45,%r49};
        add.f64         %fd16, %fd14, %fd15;
        mov.b64 {%r55,%r56}, %fd16;
        mov.u32         %r63, 1;
        shfl.down.b32 %r57, %r55, %r63, %r64;
        shfl.down.b32 %r61, %r56, %r63, %r64;
        mov.b64 %fd17, {%r57,%r61};
        add.f64         %fd4, %fd16, %fd17;
        and.b32          %r67, %r2, 31;
        setp.ne.s32        %p3, %r67, 0;
        @%p3 bra         BB_6;
        cvta.to.global.u64         %rd6, %rd12;
        ld.global.u64         %rd17, [%rd6];
BB_5:
        mov.u64         %rd8, %rd17;
        mov.b64         %fd18, %rd8;
        add.f64         %fd19, %fd4, %fd18;
        mov.b64         %rd15, %fd19;
        atom.global.cas.b64         %rd17, [%rd6], %rd8, %rd15;
        setp.ne.s64        %p4, %rd8, %rd17;
        @%p4 bra         BB_5;
BB_6:
        ret;
}
                        """

  def generateForReduce[T: ClassTag](f: (T, T) => T): Option[CUDAFunction] = {
    if (!isGPUCodeGen) { return None }

    // val declaredFields = f.getClass.getDeclaredFields
    // val declaredMethods = f.getClass.getDeclaredMethods
    // println(" + declared fields: " + declaredFields.size)
    // declaredFields.foreach { f => println("     " + f) }
    // println(" + declared methods: " + declaredMethods.size)
    // declaredMethods.foreach { m => println("     " + m) }

    val e = new reduceLambdaExpressionExtractor
    val cls = f.getClass
    classReader(cls).accept(e, 0)
    if (!e.isValid) { return None }

    val ptxReduce = e.exprtype match {
      case ILOAD => ptxIntReduce
      case DLOAD => ptxDoubleReduce
    }
    val funcEntryName = e.exprtype match {
      case ILOAD => "_intReduce"
      case DLOAD => "_doubleReduce"
    }

    val cudaFunc =
      new CUDAFunction(funcEntryName, Array("this"), Array("this"), (funcEntryName, ptxReduce))
    Some(cudaFunc)
  }
}
