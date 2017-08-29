#include <float.h>

extern "C"
__global__ void getClusterCentroids(int n, double *xs, int *cluster_index, double *c, int k, int d){
 //xs indicates datapoints, c indicates initial centroids, k indicates no. of clusters; d - dimensions
        
        int index = blockIdx.x * blockDim.x + threadIdx.x;
        if (index<n){
                double dist;
                double prevBest = DBL_MAX;
                int centroidIndex = 0;
                
                for (int clust = 0; clust < k; clust++) {
                    dist = 0.0;
                    for (int dim=0; dim<d; dim++){
                            dist += ((xs[index*d + dim]) - (c[clust*d+dim])) * ((xs[index*d + dim]) - (c[clust*d+dim]));
            }
                  
            if (dist<prevBest) {
                  prevBest = dist; centroidIndex = clust; 
            }
        }

        cluster_index[index] = centroidIndex;
    }
}

extern "C"
__global__ void getClusterCentroidsMod(int n, double *xs, double *norm, int *cluster_index, double *local_means, double *c_norm, int k, int d){
 //xs indicates datapoints, c indicates initial centroids, k indicates no. of clusters; d - dimensions

        int index = blockIdx.x * blockDim.x + threadIdx.x;
        if (index<n){
                double dist;
                double prevBest = DBL_MAX;
                int centroidIndex = 0;
                double lowerBoundOfSqDist = 0;

                for (int clust = 0; clust < k; clust++) {
                    dist = 0.0; 
                    lowerBoundOfSqDist = c_norm[clust] - norm[index];
                    lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist;
                    if (lowerBoundOfSqDist < prevBest) {
                        for (int dim=0; dim<d; dim++){
                                dist += ((xs[index*d + dim]) - (local_means[clust*d+dim])) * ((xs[index*d + dim]) - (local_means[clust*d+dim]));
                        }

                        if (dist<prevBest) {
                              prevBest = dist; centroidIndex = clust;
                        }
                    } 
                }

                cluster_index[index] = centroidIndex;
        }
}

extern "C"
__global__ void calculateIntermediates(int n, double *xs, int *cluster_index, 
   int *intermediates0, double *intermediates1, double *intermediates2, int k, int d){


    int blocksize = n / 450 + 1;
    int start = blockIdx.x * blocksize;
    int end1 = start + blocksize;
    int end;
    if (end1>n) end = n;
    else end = end1;

        if (end > n ) return;
        // loop for every K 
        for (int clust = threadIdx.y; clust < k; clust+= blockDim.y){
            // loop for every dimension(features)
            for (int dim = threadIdx.x; dim < d; dim+= blockDim.x) {

                // Calculate intermediate S0
                // for counts we don't have dimensions
                if (dim ==0) {
                    int count = 0;
                    for(int z=start; z<end; z++)
                    {
                        if(cluster_index[z] == clust) {
                        count ++;
                        }
                    }
                    intermediates0[blockIdx.x*k+clust] = count;
                }

                // Calculate intermediate S1 and S2
                double sum1 = 0.0;
                double sum2 = 0.0;
                                int idx ;
                for (int z=start; z<end; z++) {
                    if(cluster_index[z] == clust) {
			idx = z * d + dim;
                        sum1 += xs[idx];
                        sum2 += xs[idx] * xs[idx];

                    }
                }
                int index = (blockIdx.x*k*d + clust*d + dim);
	        intermediates1[index] = sum1;
                intermediates2[index] = sum2;
            }
    }
}

extern "C"
__global__ void calculateFinal(int n, int *intermediates0, double *intermediates1, 
    double *intermediates2, int *s0, double *s1, double *s2, int k, int d){

   if (blockIdx.x > 0) return;

    // Only block is invoked.        
    // loop for every K 
    for (int clust = threadIdx.y; clust < k; clust+= blockDim.y){
        // loop for every dimension(features)
        for (int dim = threadIdx.x; dim < d; dim+= blockDim.x) {

            // Calculate  S0
            // for counts we don't have dimensions
            if (dim == 0) {
                //count = 0;
                for(int z = clust; z < 450*k; z+=k){
                    {
                        s0[clust] += intermediates0[z];
                    }
                }
            }    

            // Calculate S1 and S2
	    int start = clust * d + dim;
	    int kd    = k * d;
	    double *s1end = &intermediates1[450 * kd];
	    double *s1cur = &intermediates1[start];
	    double *s2cur = &intermediates2[start];

	    for (; s1cur < s1end; s1cur += kd, s2cur += kd)
	    {
	       s1[start] += *s1cur;
	       s2[start] += *s2cur;
            }
        }
    }
}        
