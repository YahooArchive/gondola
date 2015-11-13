#!/bin/bash
rm /tmp/gondola*
/usr/bin/killall java
./bin/run.sh host2 2>&1 > target/host2 &
./bin/run.sh host3 2>&1 > target/host3 &
