#!/bin/bash

# Kill old processes
jps | grep GondolaCommand
jps | grep GondolaCommand | cut -d' ' -f1 | xargs kill -9

# Remove locks
rm -rf /tmp/gondola-lock-*

# Truncate tables
DIR="$(cd "$(dirname "$0")" && pwd)"
$DIR/mysql_server.sh drop

# Clear logs
mkdir -p logs
echo "" > logs/gondola-host1.log
echo "" > logs/gondola-host2.log
echo "" > logs/gondola-host3.log

# Start test
export CLASSPATH_PREFIX=target/test-classes:target/classes
sh target/appassembler/bin/tsunami.sh $* | tee logs/tsunami.out
