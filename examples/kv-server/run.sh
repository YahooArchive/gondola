#!/bin/bash
rm /tmp/gondola*
./bin/run host2 2>&1 > target/host2 &
pid1=$!
./bin/run host3 2>&1 > target/host3 &
pid2=$!
trap "echo 'terminate running processes...'; kill $pid1 $pid2; wait" SIGINT SIGTERM
wait
