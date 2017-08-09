#!/bin/sh
# Shell script for adding native libs. Should be used, when new versions of native libs will be released, use with care, probably you don't need to,
# except you trying to add new native libs
# # install java jars
version=0.7.5b
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcublas -Dversion=$version -Dfile=jcublas-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcuda -Dversion=$version -Dfile=jcuda-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcufft -Dversion=$version -Dfile=jcufft-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcurand -Dversion=$version -Dfile=jcurand-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusparse -Dversion=$version -Dfile=jcusparse-$version.jar -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusolver -Dversion=$version -Dfile=jcusolver-$version.jar -Durl=file://.
# # install windows-x86_64 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCublas-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas2 -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCublas2-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaDriver -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCudaDriver-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaRuntime -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCudaRuntime-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCufft -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCufft-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCurand -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCurand-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusparse2 -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCusparse2-windows-x86_64.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusparse -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCusparse-windows-x86_64.dll -Durl=file://.
# # install linux-x86 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCublas-linux-x86.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas2 -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCublas2-linux-x86.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaDriver -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCudaDriver-linux-x86.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaRuntime -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCudaRuntime-linux-x86.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCufft -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCufft-linux-x86.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCurand -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCurand-linux-x86.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusparse2 -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCusparse2-linux-x86.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusparse -Dversion=$version -Dclassifier=linux-x86 -Dfile=libJCusparse-linux-x86.so -Durl=file://.
# # install linux-x86_64 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCublas-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas2 -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCublas2-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaDriver -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCudaDriver-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaRuntime -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCudaRuntime-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCufft -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCufft-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCurand -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCurand-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusparse2 -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCusparse2-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusparse -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCusparse-linux-x86_64.so -Durl=file://.
# # install windows-x86 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCublas-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas2 -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCublas2-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaDriver -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCudaDriver-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaRuntime -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCudaRuntime-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCufft -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCufft-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCurand -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCurand-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusparse2 -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCusparse2-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusparse -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCusparse-windows-x86.dll -Durl=file://.
# install java jars for 0.6.0
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcublas -Dversion=$version -Dfile=jcublas-$version.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcuda -Dversion=$version -Dfile=jcuda-$version.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcufft -Dversion=$version -Dfile=jcufft-$version.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcurand -Dversion=$version -Dfile=jcurand-$version.jar -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=jcusparse -Dversion=$version -Dfile=jcusparse-$version.jar -Durl=file://.
# # install windows-x86 0.6.0 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCublas-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas2 -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCublas2-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaDriver -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCudaDriver-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaRuntime -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCudaRuntime-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCufft -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCufft-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCurand -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCurand-windows-x86.dll -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusparse -Dversion=$version -Dclassifier=windows-x86 -Dfile=JCusparse-windows-x86.dll -Durl=file://.
# # install windows-x86_64 0.6.0 libs
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCublas-windows-x86_64.dll -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCublas2 -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCublas2-windows-x86_64.dll -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaDriver -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCudaDriver-windows-x86_64.dll -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCudaRuntime -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCudaRuntime-windows-x86_64.dll -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCufft -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCufft-windows-x86_64.dll -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCurand -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCurand-windows-x86_64.dll -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusparse -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCusparse-windows-x86_64.dll -Durl=file://.
mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=JCusolver -Dversion=$version -Dclassifier=windows-x86_64 -Dfile=JCusolver-windows-x86_64.dll -Durl=file://.
# install linux-x86_64 0.6.5 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCublas-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas2 -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCublas2-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaDriver -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCudaDriver-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaRuntime -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCudaRuntime-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCufft -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCufft-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCurand -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCurand-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusparse -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCusparse-linux-x86_64.so -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusolver -Dversion=$version -Dclassifier=linux-x86_64 -Dfile=libJCusolver-linux-x86_64.so -Durl=file://.
# install apple-x86_64 0.6.0 libs
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCublas-apple-x86_64.dylib -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCublas2 -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCublas2-apple-x86_64.dylib -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaDriver -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCudaDriver-apple-x86_64.dylib -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCudaRuntime -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCudaRuntime-apple-x86_64.dylib -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCufft -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCufft-apple-x86_64.dylib -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCurand -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCurand-apple-x86_64.dylib -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusparse -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCusparse-apple-x86_64.dylib -Durl=file://.
# mvn deploy:deploy-file -DgroupId=jcuda -DartifactId=libJCusolver -Dversion=$version -Dclassifier=apple-x86_64 -Dfile=libJCusolver-apple-x86_64.dylib -Durl=file://.
