CUDA_PATH ?= /usr/local/cuda
CXX ?= g++
NVCC ?= $(CUDA_PATH)/bin/nvcc
COMPUTE_CAPABILITY ?= 35
CXXFLAGS ?= -m64 -O3 -Xcompiler -Wall --std=c++11 -g
NVCCFLAGS ?= -arch=sm_$(COMPUTE_CAPABILITY) -Xptxas="-v"


all : SparkGPUExamples.ptx GpuEnablerExamples.ptx LogisticRegression.ptx SparkGPUKmeans.ptx

SparkGPUExamples.ptx: SparkGPUExamples.cu
	$(NVCC) -ccbin $(CXX) $(CXXFLAGS) $(NVCCFLAGS) -ptx -o $@ -c $^

GpuEnablerExamples.ptx: GpuEnablerExamples.cu
	$(NVCC) -ccbin $(CXX) $(CXXFLAGS) $(NVCCFLAGS) -ptx -o $@ -c $^

LogisticRegression.ptx: LogisticRegression.cu
	$(NVCC) -ccbin $(CXX) $(CXXFLAGS) $(NVCCFLAGS) -ptx -o $@ -c $^

SparkGPUKmeans.ptx: SparkGPUKmeans.cu
	$(NVCC) -ccbin $(CXX) $(CXXFLAGS) $(NVCCFLAGS) -ptx -o $@ -c $^

clean:
	rm -f SparkGPUExamples.ptx GpuEnablerExamples.ptx LogisticRegression.ptx SparkGPUKmeans.ptx
