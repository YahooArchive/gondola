#!/bin/bash
rm /tmp/gondola*
export CONF="classpath:///gondola_service.conf"
./bin/run host1 2>&1 > target/host1 &
pid1=$!
./bin/run host2 2>&1 > target/host2 &
pid2=$!
./bin/run host3 2>&1 > target/host3 &
pid3=$!
./bin/run host4 2>&1 > target/host4 &
pid4=$!
./bin/run host5 2>&1 > target/host5 &
pid5=$!
./bin/run host6 2>&1 > target/host6 &
pid6=$!
trap "echo 'terminate running processes...'; kill $pid1 $pid2 $pid3 $pid4 $pid5 $pid6; wait" SIGINT SIGTERM
tail -F target/host*
wait
