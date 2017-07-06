

extern "C"
// GPU function for calculating the hypothesis function and individual gradient update for each feature of each sample
__global__ void map(int m, double *xs, double *ys, double *gradvec, double *params, int d){   // m is the no. of samples and d is the number of features in xs(input data)
	//double *h;
	//cudaMalloc (&h, m*sizeof(float));
	int index = blockIdx.x * blockDim.x + threadIdx.x;

	if (index<m){
		double accum = 0;
		//double accum = 0.0;
                for (int j=0; j<d; j++){
                        accum += xs[index*(d)+j] * params[j];
                }
		
		double h = 1.0/ (1.0 + exp(-accum));
	
//		gradvec[index*d+0] =  (h - ys[index]) * 1;

		for (int j = 0; j < d; j++){
			gradvec[index*d+j] =  (h - ys[index]) * xs[index*d+j];
		}	
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



__device__ double* deviceReduceKernelj(double * inArray, double *out, long i, long n, long length) {
    double sum = 0;
    double *inArrayBody;
    int index =  blockIdx.x * blockDim.x + threadIdx.x;
    for (long idx = index; idx < n; idx += blockDim.x * gridDim.x) {
        inArrayBody = &inArray[idx*length];
        sum += inArrayBody[i];
    }

    sum = warpReduceSum(sum);

    if ((threadIdx.x & (WARPSIZE -1)) == 0){
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

// Finds the final gradient by summing up the element-wise gradients columnwise
extern "C"
__global__
void reducegrad(int m, double *gradvec, double * sumgradvec, int d) {

    int idx = blockDim.x * blockIdx.x + threadIdx.x;

    if (idx < m)
       deviceReduceArrayKernelj(gradvec, sumgradvec, d, m);

}


