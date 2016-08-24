/* 001 */
/* 002 */ package org.apache.spark.sql.gpuenabler; // REMOVE
/* 003 */ import jcuda.Pointer;
/* 004 */ import jcuda.Sizeof;
/* 005 */ import jcuda.driver.CUdeviceptr;
/* 006 */ import jcuda.driver.CUfunction;
/* 007 */ import jcuda.driver.CUmodule;
/* 008 */ import org.apache.spark.sql.catalyst.InternalRow;
/* 009 */ import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
/* 010 */ import org.apache.spark.sql.gpuenabler.JCUDAInterface;
/* 011 */ import org.apache.spark.unsafe.types.UTF8String;
/* 012 */
/* 013 */ import java.nio.*;
/* 014 */ import java.util.Iterator;
/* 015 */ import static jcuda.driver.JCudaDriver.*;
/* 016 */ import com.ibm.gpuenabler.GPUSparkEnv;
/* 017 */
/* 018 */ public class GeneratedCode_arrayTest { // REMOVE
/* 019 */
/* 020 */   // Handle to call from compiled source
/* 021 */   public JCUDAInterface generate(Object[] references) {
/* 022 */     JCUDAInterface j = new myJCUDAInterface();
/* 023 */     return j;
/* 024 */   }
/* 025 */
/* 026 */   //Handle to directly call from JCUDAVecAdd instance for debugging.
/* 027 */   public JCUDAInterface generate() {
/* 028 */     JCUDAInterface j = new myJCUDAInterface();
/* 029 */     return j;
/* 030 */   }
/* 031 */
/* 032 */   class MemoryPointer extends CUdeviceptr {
/* 033 */     MemoryPointer() {
/* 034 */       super();
/* 035 */     }
/* 036 */     public long getNativePointer() {
/* 037 */       return super.getNativePointer();
/* 038 */     }
/* 039 */   }
/* 040 */
/* 041 */   class myJCUDAInterface extends JCUDAInterface {
/* 042 */     //Static variables
/* 043 */     private Object[] references;
/* 044 */     private UnsafeRow result;
/* 045 */     private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
/* 046 */     private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
/* 047 */     private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter;
/* 048 */     private Iterator<InternalRow> inpitr = null;
/* 049 */     private boolean processed = false;
/* 050 */     private boolean freeMemory = true;
/* 051 */     private int idx = 0;
/* 052 */     private int numElements = 0;
/* 053 */
/* 054 */
/* 055 */     private ByteBuffer gpuInputHost_cnt1;
/* 056 */     private MemoryPointer pinMemPtr_cnt1;
/* 057 */
/* 058 */     private MemoryPointer gpuInputDevice_cnt1;
/* 059 */
/* 060 */
/* 061 */     private ByteBuffer gpuInputHost_cnt2;
/* 062 */     private MemoryPointer pinMemPtr_cnt2;
/* 063 */
/* 064 */     private MemoryPointer gpuInputDevice_cnt2;
/* 065 */
/* 066 */
/* 067 */     private ByteBuffer gpuInputHost_arr;
/* 068 */     private MemoryPointer pinMemPtr_arr;
/* 069 */     private int gpuInputHost_arr_numCols;
/* 070 */     private MemoryPointer gpuInputDevice_arr;
/* 071 */
/* 072 */
/* 073 */     private ByteBuffer gpuOutputHost_sum;
/* 074 */     private MemoryPointer pinMemPtr_sum;
/* 075 */     private MemoryPointer gpuOutputDevice_sum;
/* 076 */
/* 077 */     private UTF8String directCopyHost_name[];
/* 078 */
/* 079 */
/* 080 */     public myJCUDAInterface() {
/* 081 */       result = new UnsafeRow(3);
/* 082 */       this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
/* 083 */       this.rowWriter =
/* 084 */       new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 3);
/* 085 */       arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
/* 086 */     }
/* 087 */
/* 088 */     public void init(Iterator<InternalRow> inp, int size) {
/* 089 */       inpitr = inp;
/* 090 */       numElements = size;
/* 091 */     }
/* 092 */
/* 093 */     public boolean hasNext() {
/* 094 */       if(!processed) {
/* 095 */         processGPU();
/* 096 */         processed = true;
/* 097 */       }
/* 098 */       else if(idx >= numElements) {
/* 099 */         if(freeMemory) {
/* 100 */           freePinnedMemory();
/* 101 */           freeMemory=false;
/* 102 */         }
/* 103 */         return false;
/* 104 */       }
/* 105 */       return true;
/* 106 */     }
/* 107 */
/* 108 */     private void freePinnedMemory() {
/* 109 */       cuMemFreeHost(pinMemPtr_arr);cuMemFreeHost(pinMemPtr_sum);
/* 110 */     }
/* 111 */
/* 112 */     private void allocateMemory(InternalRow r) {
/* 113 */
/* 114 */       // Allocate Host and Device variables
/* 115 */       pinMemPtr_cnt1 = new MemoryPointer();
/* 116 */
/* 117 */       cuMemAllocHost(pinMemPtr_cnt1, numElements * Sizeof.LONG);
/* 118 */       gpuInputHost_cnt1 =
/* 119 */       pinMemPtr_cnt1.getByteBuffer(0,
/* 120 */         numElements * Sizeof.LONG).order(ByteOrder.LITTLE_ENDIAN);
/* 121 */       gpuInputDevice_cnt1 = new MemoryPointer();
/* 122 */       cuMemAlloc(gpuInputDevice_cnt1, numElements * Sizeof.LONG);
/* 123 */
/* 124 */       pinMemPtr_cnt2 = new MemoryPointer();
/* 125 */
/* 126 */       cuMemAllocHost(pinMemPtr_cnt2, numElements * Sizeof.LONG);
/* 127 */       gpuInputHost_cnt2 =
/* 128 */       pinMemPtr_cnt2.getByteBuffer(0,
/* 129 */         numElements * Sizeof.LONG).order(ByteOrder.LITTLE_ENDIAN);
/* 130 */       gpuInputDevice_cnt2 = new MemoryPointer();
/* 131 */       cuMemAlloc(gpuInputDevice_cnt2, numElements * Sizeof.LONG);
/* 132 */
/* 133 */       pinMemPtr_arr = new MemoryPointer();
/* 134 */       gpuInputHost_arr_numCols = r.getArray(0).numElements();
/* 135 */       cuMemAllocHost(pinMemPtr_arr, numElements * Sizeof.LONG* gpuInputHost_arr_numCols);
/* 136 */       gpuInputHost_arr =
/* 137 */       pinMemPtr_arr.getByteBuffer(0,
/* 138 */         numElements * Sizeof.LONG* gpuInputHost_arr_numCols).order(ByteOrder.LITTLE_ENDIAN);
/* 139 */       gpuInputDevice_arr = new MemoryPointer();
/* 140 */       cuMemAlloc(gpuInputDevice_arr, numElements * Sizeof.LONG* gpuInputHost_arr_numCols);
/* 141 */
/* 142 */       pinMemPtr_sum = new MemoryPointer();
/* 143 */       cuMemAllocHost(pinMemPtr_sum, numElements * Sizeof.LONG);
/* 144 */       gpuOutputHost_sum =
/* 145 */       pinMemPtr_sum.getByteBuffer(0,
/* 146 */         numElements * Sizeof.LONG).order(ByteOrder.LITTLE_ENDIAN);
/* 147 */       gpuOutputDevice_sum = new MemoryPointer();
/* 148 */       cuMemAlloc(gpuOutputDevice_sum, numElements * Sizeof.LONG);
/* 149 */
/* 150 */       directCopyHost_name = new UTF8String[numElements];
/* 151 */
/* 152 */     }
/* 153 */
/* 154 */     public void processGPU() {
/* 155 */
/* 156 */       CUmodule module = GPUSparkEnv.get().cudaManager().getModule("/GpuEnablerExamples.ptx");
/* 157 */
/* 158 */       // Obtain a function pointer to the arrayTest function.
/* 159 */       CUfunction function = new CUfunction();
/* 160 */       cuModuleGetFunction(function, module, "arrayTest");
/* 161 */
/* 162 */
/* 163 */       // Fill GPUInput/Direct Copy Host variables
/* 164 */       for(int i=0; inpitr.hasNext();i++) {
/* 165 */         InternalRow r = (InternalRow) inpitr.next();
/* 166 */         if (i == 0)  allocateMemory(r);
/* 167 */         gpuInputHost_cnt1.putLong(r.getLong(1));
/* 168 */         gpuInputHost_cnt2.putLong(r.getLong(2));
/* 169 */
/* 170 */         long tmp[] = r.getArray(0).toLongArray();
/* 171 */         for(int j = 0; j < gpuInputHost_arr_numCols; j ++)
/* 172 */         gpuInputHost_arr.putLong(tmp[j]);
/* 173 */         directCopyHost_name[i] = r.getUTF8String(3).clone();
/* 174 */       }
/* 175 */
/* 176 */       // Flip buffer for read
/* 177 */       gpuInputHost_cnt1.flip();
/* 178 */       gpuInputHost_cnt2.flip();
/* 179 */       gpuInputHost_arr.flip();
/* 180 */
/* 181 */       // Copy data from Host to Device
/* 182 */       cuMemcpyHtoD(gpuInputDevice_cnt1,
/* 183 */         Pointer.to(gpuInputHost_cnt1),
/* 184 */         numElements * Sizeof.LONG);
/* 185 */       cuMemcpyHtoD(gpuInputDevice_cnt2,
/* 186 */         Pointer.to(gpuInputHost_cnt2),
/* 187 */         numElements * Sizeof.LONG);
/* 188 */       cuMemcpyHtoD(gpuInputDevice_arr,
/* 189 */         Pointer.to(gpuInputHost_arr),
/* 190 */         numElements * Sizeof.LONG* gpuInputHost_arr_numCols);
/* 191 */
/* 192 */
/* 193 */       Pointer kernelParameters = Pointer.to(
/* 194 */         Pointer.to(new int[]{numElements})
/* 195 */         ,Pointer.to(gpuInputDevice_cnt1)
/* 196 */         ,Pointer.to(gpuInputDevice_cnt2)
/* 197 */         ,Pointer.to(gpuInputDevice_arr)
/* 198 */         ,Pointer.to(gpuOutputDevice_sum)
/* 199 */       );
/* 200 */
/* 201 */       // Call the kernel function.
/* 202 */       int blockSizeX = 256;
/* 203 */       int gridSizeX = (int) Math.ceil((double) numElements / blockSizeX);
/* 204 */
/* 205 */       cuLaunchKernel(function,
/* 206 */         gridSizeX, 1, 1,      // Grid dimension
/* 207 */         blockSizeX, 1, 1,      // Block dimension
/* 208 */         0, null,               // Shared memory size and stream
/* 209 */         kernelParameters, null // Kernel- and extra parameters
/* 210 */       );
/* 211 */
/* 212 */
/* 213 */       cuCtxSynchronize();
/* 214 */
/* 215 */       cuMemcpyDtoH(Pointer.to(gpuOutputHost_sum), gpuOutputDevice_sum, numElements * Sizeof.LONG);
/* 216 */
/* 217 */       cuMemFree(gpuInputDevice_cnt1);
/* 218 */       cuMemFreeHost(pinMemPtr_cnt1);
/* 219 */       cuMemFree(gpuInputDevice_cnt2);
/* 220 */       cuMemFreeHost(pinMemPtr_cnt2);
/* 221 */       cuMemFree(gpuInputDevice_arr);
/* 222 */
/* 223 */       cuMemFree(gpuOutputDevice_sum);
/* 224 */     }
/* 225 */
/* 226 */     public InternalRow next() {
/* 227 */       holder.reset();
/* 228 */       rowWriter.zeroOutNullBytes();
/* 229 */
/* 230 */       int tmpCursor = holder.cursor;
/* 231 */       arrayWriter.initialize(holder,gpuInputHost_arr_numCols,4);
/* 232 */       for(int j=0;j<gpuInputHost_arr_numCols;j++)
/* 233 */       arrayWriter.write(j, gpuInputHost_arr.getLong()*10);
/* 234 */       rowWriter.setOffsetAndSize(2, tmpCursor, holder.cursor - tmpCursor);
/* 235 */       rowWriter.alignToWords(holder.cursor - tmpCursor);
/* 236 */       rowWriter.write(1,gpuOutputHost_sum.getLong());
/* 237 */       rowWriter.write(0,directCopyHost_name[idx]);
/* 238 */       result.setTotalSize(holder.totalSize());
/* 239 */       idx++;
/* 240 */       return (InternalRow) result;
/* 241 */     }
/* 242 */   }
/* 243 */ } // REMOVE
/* 244 */
