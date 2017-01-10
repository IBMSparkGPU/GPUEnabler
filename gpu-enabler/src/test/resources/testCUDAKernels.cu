#include<assert.h>
#include<math.h>

#define GET_BLOB_ADDRESS(ptr, offset)   (&((ptr)[(offset)/sizeof((ptr)[0])]))
#define GET_ARRAY_CAPACITY(ptr)     (((long *)(ptr))[0])
#define GET_ARRAY_LENGTH(ptr)       (((long *)(ptr))[1])
#define GET_ARRAY_BODY(ptr)     (&((ptr)[128/sizeof((ptr)[0])]))
#define SET_ARRAY_CAPACITY(ptr, val)    { (((long *)(ptr))[0]) = (val); }
#define SET_ARRAY_LENGTH(ptr, val)  { (((long *)(ptr))[1]) = (val); }

// very simple test kernel
extern "C"
__global__ void identity(int *size, const int *input, int *output) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {
        output[ix] = input[ix];
    }
}

extern "C"
// very simple test kernel for int array
__global__ void intArrayIdentity(int *size, const int *input, int *output, int *length) {
    const int ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {

        // copy int array
        const int *inArrayBody = &input[ix* *length];
 
        int *outArrayBody = &output[ix* *length];

        for (long i = 0; i < *length; i++) {
          outArrayBody[i] = inArrayBody[i];
        }
    }
}

extern "C"
// very simple test kernel for IntDataPoint class
__global__ void IntDataPointIdentity(int *size, const int *inputX, const int *inputY, int *outputX, int *outputY, int *length) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {
        // copy int array
        const int *inArrayBody = &inputX[ix* *length];
        int *outArrayBody = &outputX[ix* *length];

        for (long i = 0; i < *length; i++) {
          outArrayBody[i] = inArrayBody[i];
        }

        // copy int scalar value
        outputY[ix] = inputY[ix];
    }
}

extern "C"
// very simple test kernel for int array with free var
__global__ void intArrayAdd(int *size, const int *input, int *output, const int *inFreeArray, int *length) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {
        // copy int array
        const int *inArrayBody = &input[ix* *length];
        int *outArrayBody = &output[ix* *length];

        for (long i = 0; i < *length; i++) {
          outArrayBody[i] = inArrayBody[i] + inFreeArray[i];
        }
    }
}

extern "C"
// test kernel for multiple input columns
__global__ void vectorLength(int *size, const double *x, const double *y, double *len) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {
        len[ix] = sqrt(x[ix] * x[ix] + y[ix] * y[ix]);
    }
}

extern "C"
// test kernel for multiple input and multiple output columns, with different types
__global__ void plusMinus(int *size, const double *base, const float *deviation, double *a, float *b) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {
        a[ix] = base[ix] - deviation[ix];
        b[ix] = base[ix] + deviation[ix];
    }
}

extern "C"
// test kernel for two const arguments
__global__ void applyLinearFunction(int *size, const short *x, short *y, short *a, short *b) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {
        y[ix] = *a + *b * x[ix];
    }
}

extern "C"
// test kernel for custom number of blocks + const argument
// manual SIMD, to be ran on size / 8 threads, assumes size % 8 == 0
// note that key is reversed, since it's little endian
__global__ void blockXOR(int *size, const char *input, char *output, long *key) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix * 8 < *size) {
        ((long *)output)[ix] = ((const long *)input)[ix] ^ *key;
    }
}

extern "C"
// another simple test kernel
__global__ void multiplyBy2(int *size, int *in, int *out) {
    const int ix = threadIdx.x + blockIdx.x * blockDim.x;

    if (ix < *size) {
        out[ix] = in[ix] * 2;
    }
}

extern "C"
// another simple test kernel
__global__ void multiplyBy2_l(int *size, long *in, long *out) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;

    if (ix < *size) {
        out[ix] = in[ix] * 2;
    }
}

extern "C"
// another simple test kernel
__global__ void multiplyBy2_self(int *size, int *in, int *out) {
    const int ix = threadIdx.x + blockIdx.x * blockDim.x;

    if (ix < *size) {
        out[ix] = in[ix] * 2;
        in[ix] = out[ix];
    }
}

extern "C"
// test reduce kernel that sums elements
__global__ void sum(int *size, int *input, int *output, int *stage, int *totalStages) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    const int jump = 64 * 256;
    if (*stage == 0) {
        if (ix < *size) {
            assert(jump == blockDim.x * gridDim.x);
            int result = 0;
            for (long i = ix; i < *size; i += jump) {
                result += input[i];
            }
            input[ix] = result;
        }
    } else if (ix == 0) {
        const long count = (*size < (long)jump) ? *size : (long)jump;
        int result = 0;
        for (long i = 0; i < count; ++i) {
            result += input[i];
        }
        output[0] = result;
    }
}

extern "C"
// test reduce kernel that sums elements
__global__ void sum_l(int *size, long *input, long *output, int *stage, int *totalStages) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    const long jump = 64 * 256;
    if (*stage == 0) {
        if (ix < jump) {
            assert(jump == blockDim.x * gridDim.x);
            for (long long i = ix+jump; i < *size; i += jump) {
                *(input+ix) += *(input+i);
            }
        }
    } else if (ix == 0) {
        const long count = (*size < (long)jump) ? *size : (long)jump;
        long long result = 0;
        for (long i = 0; i < count; ++i) {
            result += *(input+i);
        }

        output[0] = result;
    }
}

extern "C"
// test reduce kernel that sums elements
__global__ void intArraySum(int *size, const int *input, int *output, int *length, int *stage, int *totalStages) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    const int jump = 64 * 256;
    if (*stage == 0) {
        if (ix < *size) {
            assert(jump == blockDim.x * gridDim.x);

            int *accArrayBody = const_cast<int *>(&input[ix* *length]);

            for (long i = ix + jump; i < *size; i += jump) {
                const int *inArrayBody = &input[(ix* *length) + i];

                for (long j = 0; j < *length; j++) {
                     accArrayBody[j] += inArrayBody[j];
                }
            }
        }
    } else if (ix == 0) {
        const long count = (*size < jump) ? *size : (long)jump;
        int *outArrayBody = &output[ix* *length];

        for (long i = 0; i < count; i++) {
            const int *inArrayBody = &input[(i* *length)];

            if (i == 0) {
                for (long j = 0; j < *length; j++) {
                    outArrayBody[j] = 0;
                }
            }
            for (long j = 0; j < *length; j++) {
                outArrayBody[j] += inArrayBody[j];
            }
        }
   }
}

extern "C"
// map for DataPoint class
__global__ void DataPointMap(int *size, const double *inputX, const double *inputY, double *output, const double *inFreeArray, int *length) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix < *size) {
        // copy int array
        const double *inArrayBody = &inputX[ix* *length];
        double *outArrayBody = &output[ix* *length];

        for (long i = 0; i < *length; i++) {
          outArrayBody[i] = inArrayBody[i] + inFreeArray[i];
        }
    }
}

extern "C"
// reduce for DataPoint class
__global__ void DataPointReduce(int *size, const double *input, double *output, int *length, int *stage, int *totalStages) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    const int jump = 64 * 256;
    if (*stage == 0) {
        if (ix < *size) {
            assert(jump == blockDim.x * gridDim.x);
            double *accArrayBody = const_cast<double *>(&input[ix* *length]);

            for (long i = ix + jump; i < *size; i += jump) {
                const double *inArrayBody = &input[(ix* *length) + i];

                for (long j = 0; j < *length; j++) {
                     accArrayBody[j] += inArrayBody[j];
                }
            }
        }
    } else if (ix == 0) {
        const long count = (*size < (long)jump) ? *size : (long)jump;
        double *outArrayBody = &output[ix* *length];

        for (long i = 0; i < count; i++) {
            const double *inArrayBody = &input[(i* *length)];

            if (i == 0) {
                for (long j = 0; j < *length; j++) {
                    outArrayBody[j] = 0;
                }
            }
            for (long j = 0; j < *length; j++) {
                outArrayBody[j] += inArrayBody[j];
            }
        }
   }
}

// map for Logistic regression
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

#define WARPSIZE    32

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

#if (__CUDA_ARCH__ >= 300)
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

__device__ double* deviceReduceKernel(double * inArray, double *out, long i, long n, long length) {
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

__device__ void deviceReduceArrayKernel(double * inArray, double *outputArrayBody, long length, long n) {
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
        deviceReduceKernel(inArray, &outputArrayBody[i], i, n, length);
    }
}
#endif


extern "C"
__global__
void blockReduce(int *count, double *data, double * result, int *user_D) {

    int idx = blockDim.x * blockIdx.x + threadIdx.x;
#if (__CUDA_ARCH__ >= 300)
    if (idx < *count)
       deviceReduceArrayKernel(data, result, *user_D, *count);

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

