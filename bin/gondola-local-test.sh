#!/bin/sh

function start  {
  rm -rf /tmp/gondola*
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -XX:+UnlockCommercialFeatures -XX:+FlightRecorder" nohup bin/gondola.sh -hostid host1 -clusterid cluster1 -workers  10 -conf conf/gondola-sample.conf &
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 -XX:+UnlockCommercialFeatures -XX:+FlightRecorder" nohup bin/gondola.sh -hostid host2 -clusterid cluster1 -workers  10 -conf conf/gondola-sample.conf &
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007 -XX:+UnlockCommercialFeatures -XX:+FlightRecorder" nohup bin/gondola.sh -hostid host3 -clusterid cluster1 -workers  10 -conf conf/gondola-sample.conf &
}

function stop {
  jps | grep GondolaCommand | awk '{print $1}' | xargs kill -9
  cp /dev/null nohup.out
  sleep 1
}

if [ "x$1" = 'xstart' ] ; then
  start
elif [ "x$1" = 'xstop' ] ; then
  stop
elif [ "x$1" = 'xrestart' ] ; then
  stop
  start
else
  echo "usage: $0 [start|stop|restart]"
fi
