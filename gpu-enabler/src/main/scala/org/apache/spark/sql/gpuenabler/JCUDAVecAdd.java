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

import java.nio.CharBuffer;
import java.nio.IntBuffer;
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

        //Static variables
        private Object[] references;
        private UnsafeRow result;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
        private Iterator<InternalRow> inpitr = null;
        private boolean processed = false;
        private int idx = 0;
        private int numElements = 0;

        //host input variables
        private long hostinput0[] = new long[10];
        private long hostinput1[] = new long[10];
        private UTF8String hostinput2[] = new UTF8String[10];

        //host output variables
        private long hostoutput0[] = new long[10];

        public myJCUDAInterface() {
            result = new UnsafeRow(2);
            this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
            this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 2);
        }

        public void init(Iterator<InternalRow> inp) {
            inpitr = inp;
        }

        private void processCPU() {
            for(int i=0; inpitr.hasNext();i++) {
                InternalRow r = (InternalRow) inpitr.next();
                hostinput0[i] = r.getLong(0);
                hostinput1[i] = r.getLong(1);
                hostinput2[i] = r.getUTF8String(2).clone();
                numElements++;
            }
            for(int i=0;i<numElements;i++)
                hostoutput0[i] = hostinput0[i] + hostinput1[i];

        }

        public void execute() {
            processed = true;
            processCPU();
        }

        public InternalRow next() {
            holder.reset();
            rowWriter.zeroOutNullBytes();
            rowWriter.write(0,hostinput2[idx]);
            rowWriter.write(1,hostoutput0[idx]);
            result.setTotalSize(holder.totalSize());
            idx++;
            return (InternalRow) result;
        }

        public boolean hasNext() {	
            if(!processed) execute();
            return idx < numElements;
        }

        public void processGPU() {

            String ptxFileName = "/JCudaVectorAddKernel.ptx";

            CUmodule module = GPUSparkEnv.get().cudaManager().getModule("/JCudaVectorAddKernel.ptx");

            // Obtain a function pointer to the "add" function.
            CUfunction function = new CUfunction();
            cuModuleGetFunction(function, module, "mul");


            // Allocate and fill the host hostinput data
            for(int i=0; inpitr.hasNext();i++) {
                InternalRow r = (InternalRow) inpitr.next();
                hostinput0[i] = r.getLong(0);
                hostinput1[i] = r.getLong(1);
                hostinput2[i] = r.getUTF8String(2).clone();
                numElements++;
            }

            // Allocate the device hostinput data, and copy the
            // host hostinput data to the device
            CUdeviceptr deviceinput0 = new CUdeviceptr();
            cuMemAlloc(deviceinput0, numElements * Sizeof.LONG);
            cuMemcpyHtoD(deviceinput0, Pointer.to(hostinput0),
                    numElements * Sizeof.LONG);

            CUdeviceptr deviceinput1 = new CUdeviceptr();
            cuMemAlloc(deviceinput1, numElements * Sizeof.LONG);
            cuMemcpyHtoD(deviceinput1, Pointer.to(hostinput1),
                    numElements * Sizeof.LONG);

            // Allocate device output memory
            CUdeviceptr deviceoutput0 = new CUdeviceptr();
            cuMemAlloc(deviceoutput0, numElements * Sizeof.LONG);

            // Set up the kernel parameters: A pointer to an array
            // of pointers which point to the actual values.
            Pointer kernelParameters = Pointer.to(
                    Pointer.to(new int[]{numElements}),
                    Pointer.to(deviceinput0),
                    Pointer.to(deviceinput1),
                    Pointer.to(deviceoutput0)
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
            cuMemcpyDtoH(Pointer.to(hostoutput0), deviceoutput0,
                    numElements * Sizeof.LONG);

            // Clean up.
            cuMemFree(deviceinput0);
            cuMemFree(deviceinput1);
            cuMemFree(deviceoutput0);
        }
    }


} // REMOVE

