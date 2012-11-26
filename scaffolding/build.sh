#!/bin/bash
# Simple script to build the code.
javac ./src/java/*.java -d ./bin
cp ./src/shell/* ./bin/
cp ./src/perl/*  ./bin/
chmod u+x ./bin/*.pl
chmod u+x ./bin/*.sh