#!/bin/sh
# Shell script for adding native libs. Should be used, when new versions of native libs will be released, use with care, probably you don't need to,
# except you trying to add new native libs
# # install java jars
version=0.8.0
cudnn_version=0.8.0
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcublas -Dversion=$version -Dfile=jcublas-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcuda -Dversion=$version -Dfile=jcuda-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcufft -Dversion=$version -Dfile=jcufft-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcurand -Dversion=$version -Dfile=jcurand-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusparse -Dversion=$version -Dfile=jcusparse-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusolver -Dversion=$version -Dfile=jcusolver-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jnvgraph -Dversion=$version -Dfile=jnvgraph-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcudnn -Dversion=$version -Dfile=jcudnn-$cudnn_version.jar -Durl=file://.
# # install windows-x86_64 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCublas-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas2 -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCublas2-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaDriver -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCudaDriver-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaRuntime -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCudaRuntime-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCufft -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCufft-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCurand -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCurand-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusparse -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCusparse-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusolver -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCusolver-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudnn -Dversion=$cudnn_version -Dclassifier=windows-x86_64 -Dfile=JCudnn-windows-x86_64.dll -Durl=file://.
# # install linux-x86_64 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcublas-natives -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=jcublas-natives-$version-linux-x86_64.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcuda-natives -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=jcuda-natives-$version-linux-x86_64.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcufft-natives -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=jcufft-natives-$version-linux-x86_64.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcurand-natives -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=jcurand-natives-$version-linux-x86_64.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusparse-natives -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=jcusparse-natives-$version-linux-x86_64.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusolver-natives -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=jcusolver-natives-$version-linux-x86_64.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcudnn-natives -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=jcudnn-natives-$version-linux-x86_64.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jnvgraph-natives -Dversion=$cudnn_version -Dclassifier=linux-x86_64 -Dfile=jnvgraph-natives-$version-linux-x86_64.jar -Durl=file://.
# install apple-x86_64
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcublas-natives -Dversion=$version -Dclassifier=apple-x86_64  -Dfile=jcublas-natives-$version-apple-x86_64.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcuda-natives -Dversion=$version -Dclassifier=apple-x86_64  -Dfile=jcuda-natives-$version-apple-x86_64.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcufft-natives -Dversion=$version -Dclassifier=apple-x86_64  -Dfile=jcufft-natives-$version-apple-x86_64.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcurand-natives -Dversion=$version -Dclassifier=apple-x86_64  -Dfile=jcurand-natives-$version-apple-x86_64.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusparse-natives -Dversion=$version -Dclassifier=apple-x86_64  -Dfile=jcusparse-natives-$version-apple-x86_64.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusolver-natives -Dversion=$version -Dclassifier=apple-x86_64  -Dfile=jcusolver-natives-$version-apple-x86_64.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcudnn-natives -Dversion=$version -Dclassifier=apple-x86_64  -Dfile=jcudnn-natives-$version-apple-x86_64.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jnvgraph-natives -Dversion=$cudnn_version -Dclassifier=apple-x86_64  -Dfile=jnvgraph-natives-$version-apple-x86_64.jar -Durl=file://.
# # install linux-ppc libs
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCublas-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas2 -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCublas2-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaDriver -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCudaDriver-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaRuntime -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCudaRuntime-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCufft -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCufft-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCurand -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCurand-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusparse -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCusparse-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusolver -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJCusolver-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudnn -Dversion=$cudnn_version -Dclassifier=linux-ppc64 -Dfile=libJCudnn-linux-ppc_64.so -Durl=file://.
#mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJNvgraph -Dversion=$version -Dclassifier=linux-ppc64 -Dfile=libJNvrtc-$version-ppc_64.so -Durl=file://.
