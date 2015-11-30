#!/bin/sh

function start {
  rm -rf /tmp/gondola*
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" nohup bin/gondola -hostid host1 -clusterid cluster1 -port 1099 -conf conf/gondola-sample.conf start 2>&1
  sleep 1
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006" nohup bin/gondola -hostid host2 -clusterid cluster1 -port 1100 -conf conf/gondola-sample.conf start 2>&1
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007" nohup bin/gondola -hostid host3 -clusterid cluster1 -port 1101 -conf conf/gondola-sample.conf start 2>&1
  echo "Wait for server up and send force leader signal.."
  sleep 1
  echo "F" | nc localhost 1099
  echo "c 1" | nc localhost 1099
  echo "c 2" | nc localhost 1099
  echo "c 3" | nc localhost 1099
}

function stop {
  jps | grep GondolaCommand | awk '{print $1}' | xargs kill -9
  cp /dev/null nohup.out
  sleep 1
}

function restart {
  stop
  start
}
if [ "x$1" = 'xstart' ]; then
  echo "start gondola server for test.."
  start
elif [ "x$1" = 'xstop' ]; then
  echo "stop gondola server for test.."
  stop
elif [ "x$1" = 'xrestart' ]; then
  echo "restart gondola server for test.."
  restart
else
  echo "usage: $0 [start|stop|restart]"
fi
