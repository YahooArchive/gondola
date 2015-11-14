#!/bin/bash
rm /tmp/gondola*
/usr/bin/killall java
./bin/run host2 2>&1 > target/host2 &
./bin/run host3 2>&1 > target/host3 &
