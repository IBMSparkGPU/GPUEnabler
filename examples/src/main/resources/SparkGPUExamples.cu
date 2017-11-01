

#include <assert.h>
#include <math.h>
#include <curand_kernel.h>

#define GET_BLOB_ADDRESS(ptr, offset) (&((ptr)[(offset)/sizeof((ptr)[0])]))
#define GET_ARRAY_CAPACITY(ptr)   (((long *)(ptr))[0])
#define GET_ARRAY_LENGTH(ptr)   (((long *)(ptr))[1])
#define GET_ARRAY_BODY(ptr)   (&((ptr)[128/sizeof((ptr)[0])]))
#define SET_ARRAY_CAPACITY(ptr, val)  { (((long *)(ptr))[0]) = (val); }
#define SET_ARRAY_LENGTH(ptr, val)  { (((long *)(ptr))[1]) = (val); }

__global__ void SparkGPUPi_map(const int *input, int *output, long size) {
  const int idx = threadIdx.x + blockIdx.x * blockDim.x;
  curandState s;
  int seed = 1;
  curand_init(seed, idx, 0, &s);

  float x = curand_uniform(&s) * 2 - 1;
  float y = curand_uniform(&s) * 2 - 1;
  if (x * x + y * y < 1) {
    output[idx] = 1;
  } else {
    output[idx] = 0;
  }
}

__global__ void SparkGPUPi_reduce(int *input, int *output, long size, int stage, int totalStages) {
  const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
  const int jump = 64 * 256;
  if (stage == 0) {
    assert(jump == blockDim.x * gridDim.x);
    int result = 0;
    for (long i = ix; i < size; i += jump) {
      result += input[i];
    }
    input[ix] = result;
  } else if (ix == 0) {
    int result = 0;
    for (long i = 0; i < jump; ++i) {
      result += input[i];
    }
    output[0] = result;
  }
}

__device__ double sdotvv(const double * __restrict__ x, const double * __restrict__ y, int n) {
    double ans = 0.0;
    for(int i = 0; i < n; i++) {
        ans += x[i] * y[i];
    }
    return ans;
}
__device__ void dmulvs(double *result, const double * __restrict__ x, double c, int n) {
    for(int i = 0; i < n; i++) {
        result[i] = x[i] * c;
    }
}
__device__ void map(double *result, const double * __restrict__ x, double y, const double * __restrict__ w, int n) {
    dmulvs(result, x, (1 / (1 + exp(-y * (sdotvv(w, x, n)))) - 1) * y, n);
}

__global__ void SparkGPULR_map(const long * __restrict__ inputX, const double *  __restrict__ inputY, const double * __restrict__ inputBlob, long *output, double *outputBlob, long size, const double * __restrict__ inputW) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < size) {
        // copy int array
        long offset = inputX[ix];
        const double *inArray = GET_BLOB_ADDRESS(inputBlob, offset);
        const long capacity = GET_ARRAY_CAPACITY(inArray);
        const long length   = GET_ARRAY_LENGTH(inArray);
        const double * inArrayBody = GET_ARRAY_BODY(inArray);

        double *outArray = GET_BLOB_ADDRESS(outputBlob, offset);
        double *outArrayBody = GET_ARRAY_BODY(outArray);

        map(outArrayBody, inArrayBody, inputY[ix], inputW, length);

        output[ix] = offset;
        SET_ARRAY_CAPACITY(outArray, capacity);
        SET_ARRAY_LENGTH(outArray, length);
    }
}

#define WARPSIZE  32

__device__ inline double atomicAddDouble(double *address, double val) {
  unsigned long long int *address_as_ull = (unsigned long long int *)address;
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed  = old;
    old = atomicCAS(address_as_ull, assumed,
                    __double_as_longlong(val + 
                      __longlong_as_double(assumed)));
  } while (assumed != old);

  return __longlong_as_double(old);
}

__device__ inline double __shfl_double(double d, int lane) {
  // Split the double number into 2 32b registers.
  int lo, hi;
  asm volatile("mov.b64 {%0,%1}, %2;" : "=r"(lo), "=r"(hi) : "d"(d));

  // Shuffle the two 32b registers.
  lo = __shfl(lo, lane);
  hi = __shfl(hi, lane);

  // Recreate the 64b number.
  asm volatile("mov.b64 %0, {%1,%2};" : "=d"(d) : "r"(lo), "r"(hi));
  return d;
}

__device__ inline double warpReduceSum(double val) {
  int i = blockIdx.x  * blockDim.x + threadIdx.x;
#pragma unroll
  for (int offset = WARPSIZE / 2; offset > 0; offset /= 2) {
     val += __shfl_double(val, (i + offset) % WARPSIZE);
  }
  return val;
}

__device__ inline double4 __shfl_double4(double4 d, int lane) {
  // Split the double number into 2 32b registers.
  int lox, loy, loz, low, hix, hiy, hiz, hiw;
  asm volatile("mov.b64 {%0,%1}, %2;" : "=r"(lox), "=r"(hix) : "d"(d.x));
  asm volatile("mov.b64 {%0,%1}, %2;" : "=r"(loy), "=r"(hiy) : "d"(d.y));
  asm volatile("mov.b64 {%0,%1}, %2;" : "=r"(loz), "=r"(hiz) : "d"(d.z));
  asm volatile("mov.b64 {%0,%1}, %2;" : "=r"(low), "=r"(hiw) : "d"(d.w));

  // Shuffle the two 32b registers.
  lox = __shfl(lox, lane);
  hix = __shfl(hix, lane);
  loy = __shfl(loy, lane);
  hiy = __shfl(hiy, lane);
  loz = __shfl(loz, lane);
  hiz = __shfl(hiz, lane);
  low = __shfl(low, lane);
  hiw = __shfl(hiw, lane);

  // Recreate the 64b number.
  asm volatile("mov.b64 %0, {%1,%2};" : "=d"(d.x) : "r"(lox), "r"(hix));
  asm volatile("mov.b64 %0, {%1,%2};" : "=d"(d.y) : "r"(loy), "r"(hiy));
  asm volatile("mov.b64 %0, {%1,%2};" : "=d"(d.z) : "r"(loz), "r"(hiz));
  asm volatile("mov.b64 %0, {%1,%2};" : "=d"(d.w) : "r"(low), "r"(hiw));
  return d;
}

__device__ inline double4 warpReduceVSum(double4 val4) {
  int i = blockIdx.x  * blockDim.x + threadIdx.x;
#pragma unroll
  for (int offset = WARPSIZE / 2; offset > 0; offset /= 2) {
     double4 shiftedVal4 = __shfl_double4(val4, (i + offset) % WARPSIZE);
     val4.x += shiftedVal4.x;
     val4.y += shiftedVal4.y;
     val4.z += shiftedVal4.z;
     val4.w += shiftedVal4.w;
  }
  return val4;
}

__device__ double* deviceReduceKernel(const long * __restrict__ input, const double * __restrict__ inputBlob, double *out, long i, long n) {
    double sum = 0;
    for (long idx = blockIdx.x * blockDim.x + threadIdx.x; idx < n; idx += blockDim.x * gridDim.x) {
        const long offset = input[idx];
        const double * __restrict__ inArray = GET_BLOB_ADDRESS(inputBlob, offset);
        const double * __restrict__ inArrayBody = GET_ARRAY_BODY(inArray);
        sum += inArrayBody[i];
    }

    sum = warpReduceSum(sum);

    if ((threadIdx.x & (WARPSIZE - 1)) == 0) { 
        atomicAddDouble(out, sum);
    }
    return out;
}

__device__ void deviceReduceArrayKernal(const long * __restrict__ input, const double * __restrict__ inputBlob, double *outputArrayBody, long length, long n) {
    long i = 0;

    // unrolled version
    while ((length - i) >= 4) {
        double4 sum4;
        sum4.x = 0; sum4.y = 0; sum4.z = 0; sum4.w = 0;
        for (long idx = blockIdx.x * blockDim.x + threadIdx.x; idx < n; idx += blockDim.x * gridDim.x) {
            const long offset = input[idx];
            const double * __restrict__ inArray = GET_BLOB_ADDRESS(inputBlob, offset);
            const double * __restrict__ inArrayBody = GET_ARRAY_BODY(inArray);
            sum4.x += inArrayBody[i];
            sum4.y += inArrayBody[i+1];
            sum4.z += inArrayBody[i+2];
            sum4.w += inArrayBody[i+3];
        }

        sum4 = warpReduceVSum(sum4);

        double *outx = &outputArrayBody[i];
        double *outy = &outputArrayBody[i+1];
        double *outz = &outputArrayBody[i+2];
        double *outw = &outputArrayBody[i+3];
        if ((threadIdx.x & (WARPSIZE - 1)) == 0) { 
            atomicAddDouble(outx, sum4.x);
            atomicAddDouble(outy, sum4.y);
            atomicAddDouble(outz, sum4.z);
            atomicAddDouble(outw, sum4.w);
        }
        i += 4;
    }

    for (; i < length; i++) {
        deviceReduceKernel(input, inputBlob, &outputArrayBody[i], i, n);
    }
}

__global__ void SparkGPULR_reduce(const long * __restrict__ input, const double * __restrict__ inputBlob, long *output, double *outputBlob, long size, int stage, int totalStages) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
#if (__CUDA_ARCH__ >= 300)
    if ((stage == 0) && (idx < size)) {
        const double * __restrict__ inArray = GET_BLOB_ADDRESS(inputBlob, input[idx]);
        const long inArrayCapacity = GET_ARRAY_CAPACITY(inArray);
        const long inArrayLength = GET_ARRAY_LENGTH(inArray);
        output[0] = 0;
        double *outArray = GET_BLOB_ADDRESS(outputBlob, output[0]);
        double *outArrayBody = GET_ARRAY_BODY(outArray);
        if (idx < inArrayLength) {
          outArrayBody[idx] = 0;
        }

        deviceReduceArrayKernal(input, inputBlob, outArrayBody, inArrayLength, size);

        SET_ARRAY_CAPACITY(outArray, inArrayCapacity);
        SET_ARRAY_LENGTH(outArray, inArrayLength);
    }
#else
    if ((stage == 0) && (idx == 0)) {
        output[idx] = 0;
        double *outArray = GET_BLOB_ADDRESS(outputBlob, output[idx]);
        double *outArrayBody = GET_ARRAY_BODY(outArray);
 
        long capacity = 0, length = 0;
        for (long i = 0; i < size; i++) {
            long offset = input[i];
            const double *inArray = GET_BLOB_ADDRESS(inputBlob, offset);
            capacity = GET_ARRAY_CAPACITY(inArray);
            length   = GET_ARRAY_LENGTH(inArray);
            const double * __restrict__ inArrayBody = GET_ARRAY_BODY(inArray);

            if (i == 0) {
                for (long j = 0; j < length; j++) {
                    outArrayBody[j] = 0;
                }
            } 

            for (long j = 0; j < length; j++) {
                outArrayBody[j] += inArrayBody[j];
            }
        }
        SET_ARRAY_CAPACITY(outArray, capacity);
        SET_ARRAY_LENGTH(outArray, length);
    }
#endif

}

__device__ double* deviceReduceKernelj(double * inArray, double *out, long i, long n, long length) {
    double sum = 0;
    double *inArrayBody;
    for (long idx = blockIdx.x * blockDim.x + threadIdx.x; idx < n; idx += blockDim.x * gridDim.x) {
        inArrayBody = &inArray[idx*length];
        sum += inArrayBody[i];
    }

    sum = warpReduceSum(sum);

    if ((threadIdx.x & (WARPSIZE - 1)) == 0) {
        atomicAddDouble(out, sum);
    }
    return out;
}

__device__ void deviceReduceArrayKernelj(double * inArray, double *outputArrayBody, long length, long n) {
    long i = 0;
    double *inArrayBody;

    // unrolled version
    while ((length - i) >= 4) {
        double4 sum4;
        sum4.x = 0; sum4.y = 0; sum4.z = 0; sum4.w = 0;
        for (long idx = blockIdx.x * blockDim.x + threadIdx.x; idx < n; idx += blockDim.x * gridDim.x) {
            inArrayBody = &inArray[idx*length];
            sum4.x += inArrayBody[i];
            sum4.y += inArrayBody[i+1];
            sum4.z += inArrayBody[i+2];
            sum4.w += inArrayBody[i+3];
        }

        sum4 = warpReduceVSum(sum4);

        if ((threadIdx.x & (WARPSIZE - 1)) == 0) {

        double *outx = &outputArrayBody[i];
        double *outy = &outputArrayBody[i+1];
        double *outz = &outputArrayBody[i+2];
        double *outw = &outputArrayBody[i+3];
            atomicAddDouble(outx, sum4.x);
            atomicAddDouble(outy, sum4.y);
            atomicAddDouble(outz, sum4.z);
            atomicAddDouble(outw, sum4.w);
        }
        i += 4;
    }

    for (; i < length; i++) {
        deviceReduceKernelj(inArray, &outputArrayBody[i], i, n, length);
    }
}


extern "C"
__global__
void blockReduce(int *count, double *data, double * result, int *user_D) {

    int idx = blockDim.x * blockIdx.x + threadIdx.x;
#if (__CUDA_ARCH__ >= 300)
    if (idx < *count)
       deviceReduceArrayKernelj(data, result, *user_D, *count);

#else
    printf("not supported");
#endif
}


extern "C"
__global__ void
mapAll(int *count, double *x, double *y, double *result, double *w, int *user_D) {
    int idx = threadIdx.x + blockIdx.x * blockDim.x;

    if(idx < *count)
        map(&result[idx * *user_D], &x[idx * *user_D ], y[idx],w, *user_D);

}

extern "C"
__global__
void dsblockReduce(int count, double *data, double * result, int user_D) {

    int idx = blockDim.x * blockIdx.x + threadIdx.x;
#if (__CUDA_ARCH__ >= 300)
    if (idx < count)
       deviceReduceArrayKernelj(data, result, user_D, count);

#else
    printf("not supported");
#endif
}

extern "C"
__global__ void
dsmapAll(int count, double *x, double *y, double *result, double *w, int user_D) {
    int idx = threadIdx.x + blockIdx.x * blockDim.x;

    if(idx < count)
        map(&result[idx * user_D], &x[idx * user_D ], y[idx],w, user_D);

}
