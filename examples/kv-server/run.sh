#!/bin/bash 
rm /tmp/gondola*
/usr/bin/killall java
./run.sh host2 2>&1 > host2 &
./run.sh host3 2>&1 > host3 &
