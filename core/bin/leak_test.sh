#!/bin/bash

# Kill old processes
jps | grep LeakTest
jps | grep LeakTest | cut -d' ' -f1 | xargs kill -9

# Remove locks
rm -rf /tmp/gondola-lock-*

# Delete tables
rm -rf /tmp/gondola-db-host*

# Clear logs
mkdir -p logs
echo "" > logs/leaktest-host1.log
echo "" > logs/leaktest-host2.log
echo "" > logs/leaktest-host3.log

# Start test
export CLASSPATH_PREFIX=target/test-classes:target/classes
sh target/appassembler/bin/leak_test.sh $* | tee logs/leak_test.out
