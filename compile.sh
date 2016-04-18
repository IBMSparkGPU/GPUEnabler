#!/bin/bash

if [ -z "$SPARK_HOME" ]; then
    echo "Please set SPARK_HOME"
    exit 1
fi  

MVN_CMD="$SPARK_HOME/build/mvn --force"
MVN_ARGS="-Dmaven.compiler.showWarnings=true -Dmaven.compiler.showDeprecation=true"

echo `pwd`
$MVN_CMD -T 2 $MVN_ARGS -DskipTests clean install $@ 2>&1 | tee ~/compile.txt

# ./utils/embed.sh -d  gpu-enabler-0.1.0.jar

