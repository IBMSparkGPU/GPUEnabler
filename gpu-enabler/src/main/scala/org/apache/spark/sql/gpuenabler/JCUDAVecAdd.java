package org.apache.spark.sql.gpuenabler; // REMOVE

import com.ibm.gpuenabler.GPUSparkEnv;
import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUdeviceptr;
import jcuda.driver.CUfunction;
import jcuda.driver.CUmodule;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.gpuenabler.JCUDAInterface;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Iterator;

import static jcuda.driver.JCudaDriver.*;

/**
 * Created by madhusudanan on 27/07/16.
 */
public class JCUDAVecAdd { // REMOVE

    // Handle to call from compiled source
    public JCUDAInterface generate(Object[] references) {
        JCUDAInterface j = new myJCUDAInterface();
        return j;
    }

    //Handle to directly call from JCUDAVecAdd instance for debugging.
    public JCUDAInterface generate() {
        JCUDAInterface j = new myJCUDAInterface();
        return j;
    }

    class myJCUDAInterface extends JCUDAInterface {

        private Object[] references;
        private UnsafeRow result;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
        private Iterator<InternalRow> inpitr = null;
        private long input0[] = new long[10];
        private long input1[] = new long[10];
        private UTF8String input2[] = new UTF8String[10];
        private long input3[] = new long[10];
        private boolean processed = false;
        private int idx = 0, totRows = 0;

        public myJCUDAInterface() {
            result = new UnsafeRow(4);
            this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
            this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 4);
        }

        public void init(Iterator<InternalRow> inp) {
            inpitr = inp;
        }

        public void process() {
            processed = true;
            for(int i=0; inpitr.hasNext();i++) {
                InternalRow r = (InternalRow) inpitr.next();
                input0[i] = r.getLong(0);
                input1[i] = r.getLong(1);
                input2[i] = r.getUTF8String(2).clone();
                input3[i] = input0[i] + input1[i];
                System.out.println("Process " + i + input3[i]);
                totRows++;
            }
        }

        public InternalRow next() {
            holder.reset();
            rowWriter.zeroOutNullBytes();
            System.out.println("next " + idx + input3[idx]);
            rowWriter.write(0,input0[idx]);
            rowWriter.write(1,input1[idx]);
            rowWriter.write(2,input2[idx]);
            rowWriter.write(3,input3[idx]);
            System.out.println("next " + idx + input2[idx]);
            idx++;
            result.setTotalSize(holder.totalSize());
            return (InternalRow) result;
        }

        public boolean hasNext() {
            if(!processed) process();
            return idx < totRows;
        }

        public InternalRow[] processGPU(InternalRow inp[]) {

            String ptxFileName = "/JCudaVectorAddKernel.ptx";

            CUmodule module = GPUSparkEnv.get().cudaManager().getModule("/JCudaVectorAddKernel.ptx");

            // Obtain a function pointer to the "add" function.
            CUfunction function = new CUfunction();
            cuModuleGetFunction(function, module, "add");

            int numElements = 100000;

            // Allocate and fill the host input data
            float hostInputA[] = new float[numElements];
            float hostInputB[] = new float[numElements];
            for (int i = 0; i < numElements; i++) {
                hostInputA[i] = (float) i;
                hostInputB[i] = (float) i;
            }

            // Allocate the device input data, and copy the
            // host input data to the device
            CUdeviceptr deviceInputA = new CUdeviceptr();
            cuMemAlloc(deviceInputA, numElements * Sizeof.FLOAT);
            cuMemcpyHtoD(deviceInputA, Pointer.to(hostInputA),
                    numElements * Sizeof.FLOAT);

            CUdeviceptr deviceInputB = new CUdeviceptr();
            cuMemAlloc(deviceInputB, numElements * Sizeof.FLOAT);
            cuMemcpyHtoD(deviceInputB, Pointer.to(hostInputB),
                    numElements * Sizeof.FLOAT);

            // Allocate device output memory
            CUdeviceptr deviceOutput = new CUdeviceptr();
            cuMemAlloc(deviceOutput, numElements * Sizeof.FLOAT);

            // Set up the kernel parameters: A pointer to an array
            // of pointers which point to the actual values.
            Pointer kernelParameters = Pointer.to(
                    Pointer.to(new int[]{numElements}),
                    Pointer.to(deviceInputA),
                    Pointer.to(deviceInputB),
                    Pointer.to(deviceOutput)
            );

            // Call the kernel function.
            int blockSizeX = 256;
            int gridSizeX = (int) Math.ceil((double) numElements / blockSizeX);
            cuLaunchKernel(function,
                    gridSizeX, 1, 1,      // Grid dimension
                    blockSizeX, 1, 1,      // Block dimension
                    0, null,               // Shared memory size and stream
                    kernelParameters, null // Kernel- and extra parameters
            );
            cuCtxSynchronize();

            // Allocate host output memory and copy the device output
            // to the host.
            float hostOutput[] = new float[numElements];
            cuMemcpyDtoH(Pointer.to(hostOutput), deviceOutput,
                    numElements * Sizeof.FLOAT);

            // Verify the result
            boolean passed = true;
            for (int i = 0; i < numElements; i++) {
                float expected = i + i;
                if (Math.abs(hostOutput[i] - expected) > 1e-5) {
                    System.out.println(
                            "At index " + i + " found " + hostOutput[i] +
                                    " but expected " + expected);
                    passed = false;
                    break;
                }
            }
            System.out.println("Test " + (passed ? "PASSED" : "FAILED"));
            // Clean up.
            cuMemFree(deviceInputA);
            cuMemFree(deviceInputB);
            cuMemFree(deviceOutput);
            return inp;
        }
    }


} // REMOVE

