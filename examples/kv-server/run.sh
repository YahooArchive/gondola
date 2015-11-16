#!/bin/bash
rm /tmp/gondola*
./bin/run host1 2>&1 > target/host1 &
pid1=$!
./bin/run host2 2>&1 > target/host2 &
pid2=$!
./bin/run host3 2>&1 > target/host3 &
pid3=$!
trap "echo 'terminate running processes...'; kill $pid1 $pid2 $pid3; wait" SIGINT SIGTERM
tail -F target/host1 target/host2 target/host3
wait
