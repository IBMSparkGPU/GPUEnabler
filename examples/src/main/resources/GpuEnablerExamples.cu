
extern "C"
// another simple test kernel
__global__ void multiplyBy2(int *size, int *in, int *out) {
    const int ix = threadIdx.x + blockIdx.x * blockDim.x;

    if (ix < *size) {
        out[ix] = in[ix] * 2;
    }
}


extern "C"
// test reduce kernel that sums elements
__global__ void sum(int *size, int *input, int *output, int *stage, int *totalStages) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    const int jump = 64 * 256;
    // if (ix == 0) printf("size: %d stage : %d totalStages : %d \n",*size, *stage, *totalStages);
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
__global__ void sum1(int *size, int *input, int *output) {
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;
    if (ix == 0) {
        int result = 0;
        for (long i = ix; i < *size; i++) {
            result += input[i];
        }
        output[0] = result;
    }
}


extern "C"
__global__ void add(int n, long *a, long *b, long *sum)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i<n)
    {
        sum[i] = a[i] + b[i];
        printf("CUDA KERNEL ADD %ld + %ld = %ld \n",a[i],b[i],sum[i]);
    }

}

extern "C"
__global__ void mul(int n, long *a, long *b, long *sum)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i<n)
    {
        sum[i] = a[i] * b[i];
        printf("CUDA KERNEL MUL %ld * %ld = %ld \n",a[i],b[i],sum[i]);
    }

}

extern "C"
__global__ void arrayTest(int n, long *factor, long *arr,int *const_arr1, long *const_arr2,long *result)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if(i == 0) {
       printf("In ArrayTest n=%d factor=%p arr=%p result=%p \n",n,factor,arr,result);
       printf("In const %d %d %d\n",const_arr1[0],const_arr1[1],const_arr1[2]);
       printf("In const %ld %ld %ld\n",const_arr2[0],const_arr2[1],const_arr2[2]);
    }

    if (i<n)
    {
        int idx = i * 3;
        result[idx]=arr[idx] * factor[i];
        result[idx + 1]=arr[idx + 1] * factor[i];
        result[idx + 2]=arr[idx + 2] * factor[i];
        printf("ArrayTest  [%ld] * [%ld %ld %ld] = [%ld %ld %ld] \n", factor[i],
                 arr[idx],arr[idx+1],arr[idx+2],
                result[idx],result[idx+1],result[idx+2]);
    }

}

extern "C"
__global__ void arrayMult(int n, long *factor, long *arr,int *const_arr1, long *const_arr2,long *result)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if(i == 0) {
       printf("In ArrayTest n=%d factor=%p arr=%p result=%p \n",n,factor,arr,result);
       printf("In const %d %d %d\n",const_arr1[0],const_arr1[1],const_arr1[2]);
       printf("In const %ld %ld %ld\n",const_arr2[0],const_arr2[1],const_arr2[2]);
    }

    if (i<n)
    {
        int idx = i * 3;
        result[idx]=arr[idx] * factor[i];
        result[idx + 1]=arr[idx + 1] * factor[i];
        result[idx + 2]=arr[idx + 2] * factor[i];
        printf("ArrayTest  [%ld] * [%ld %ld %ld] = [%ld %ld %ld] \n", factor[i],
                 arr[idx],arr[idx+1],arr[idx+2],
                result[idx],result[idx+1],result[idx+2]);
    }

}


extern "C"
__global__ void arrayTestMod(int n, long *factor, long *arr, long *result, int *const_arr1, long *const_arr2)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if(i == 0) {
       printf("In ArrayTest n=%d factor=%p arr=%p result=%p \n",n,factor,arr,result);
       printf("In const %d %d %d\n",const_arr1[0],const_arr1[1],const_arr1[2]);
       printf("In const %ld %ld %ld\n",const_arr2[0],const_arr2[1],const_arr2[2]);
       int idx = i * 3;
       result[idx]=arr[idx] * factor[i];
       printf("ArrayTest  [%ld] * [%ld %ld %ld] = [%ld] \n", factor[i],
          arr[idx],arr[idx+1],arr[idx+2],
                result[idx]);
    }
}

extern "C"
__global__ void arrayTestModStages(int n, long *factor, long *arr, long *result, int *const_arr1, long *const_arr2, 
     long *const_fixed, int outputArraySize, int stage, int totalstages)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if(i == 0) {
       printf("N : %d, stage: %d, totalstages: %d \n", n, stage, totalstages); 
       printf("In outputArraySize : %d \n", outputArraySize);
       printf("In ArrayTest n=%d factor=%p arr=%p result=%p \n",n,factor,arr,result);
       printf("In Fixed const %ld .. %ld \n",const_fixed[0], const_fixed[34]);
       printf("In const %d %d %d\n",const_arr1[0],const_arr1[1],const_arr1[2]);
       printf("In const %ld %ld %ld\n",const_arr2[0],const_arr2[1],const_arr2[2]);
       int idx = i * 3;
       result[idx]=arr[idx] * factor[i];
       printf("ArrayTest  [%ld] * [%ld %ld %ld] = [%ld] \n", factor[i],
          arr[idx],arr[idx+1],arr[idx+2],
                result[idx]);
    }
}



extern "C"
// another simple test kernel
__global__ void multiplyBy2_self(int size, long *inout) {
    const int ix = threadIdx.x + blockIdx.x * blockDim.x;

    if (ix < size) {
        inout[ix] = inout[ix] * 2;
    }
}
