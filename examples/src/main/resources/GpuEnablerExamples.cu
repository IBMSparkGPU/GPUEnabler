
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
__global__ void arrayTest(int n, long *a, long *b, long *arr,long *sum)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if(i == 0) {
       printf("In ArrayTest n=%d a=%p b=%p out=%p arr=%p\n",n,a,b,sum,arr);
    }

    if (i<n)
    {
        sum[i] = a[i] * b[i];
        printf("CUDA KERNEL ArrayTest a=%ld b=%ld sum=%ld arr=[%ld %ld %ld]\n",a[i],b[i],sum[i],arr[i*3],arr[(i*3)+1],arr[(i*3)+2]);
    }

}
