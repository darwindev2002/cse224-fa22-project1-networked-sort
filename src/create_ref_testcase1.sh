#!/bin/bash

##Remove old references
rm -f INPUT OUTPUT REF_OUTPUT MY_OUTPUT DIFF

cp testcases/testcase1/input-0.dat testcases/testcase1/INPUT
cat testcases/testcase1/input-1.dat >> testcases/testcase1/INPUT
cat testcases/testcase1/input-2.dat >> testcases/testcase1/INPUT
cat testcases/testcase1/input-3.dat >> testcases/testcase1/INPUT

cp testcases/testcase1/output-0.dat testcases/testcase1/OUTPUT
cat testcases/testcase1/output-1.dat >> testcases/testcase1/OUTPUT
cat testcases/testcase1/output-2.dat >> testcases/testcase1/OUTPUT
cat testcases/testcase1/output-3.dat >> testcases/testcase1/OUTPUT

utils/m1-arm64/bin/showsort testcases/testcase1/INPUT | sort > testcases/testcase1/REF_OUTPUT
utils/m1-arm64/bin/showsort testcases/testcase1/OUTPUT > testcases/testcase1/MY_OUTPUT

diff testcases/testcase1/REF_OUTPUT testcases/testcase1/my_output > testcases/testcase1/DIFF

