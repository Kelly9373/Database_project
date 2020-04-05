#!/usr/bin/env bash

WORKING_DIR=`pwd`
CLASSPATH="$WORKING_DIR:$WORKING_DIR/classes:$WORKING_DIR/lib:."
COMPONENT="$WORKING_DIR"

export CLASSPATH
export COMPONENT

echo "Query environment setup successfully!"

mkdir -p ${COMPONENT}/classes

javac -d ${COMPONENT}/classes ${COMPONENT}/lib/java_cup/runtime/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/utils/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/parser/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/operators/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/qp/optimizer/*.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/QueryMain.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/ConvertTxtToTbl.java
javac -d ${COMPONENT}/classes ${COMPONENT}/src/RandomDB.java

echo "Compiled Successfully"

