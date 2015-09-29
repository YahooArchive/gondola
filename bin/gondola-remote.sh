#!/bin/sh
PATH=/usr/bin:/usr/local/bin:/home/y/bin:/home/y/sbin:/home/y/bin64:/home/y/sbin64:/usr/sbin:/usr/local/sbin:/bin/:/sbin:/usr/local/apache-maven/bin/:/Users/wcpan2/google-cloud-sdk/bin/:/home/y/bin:/home/y/sbin:/Applications/Muse:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/X11/bin:/Applications/Muse

function start {
  rm -rf /tmp/gondola*
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" nohup bin/gondola.sh -hostid host1 -clusterid cluster1 -port 1099 -config conf/gondola-remote.conf start 2>&1
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006" nohup bin/gondola.sh -hostid host2 -clusterid cluster1 -port 1100 -config conf/gondola-remote.conf start 2>&1
  env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007" nohup bin/gondola.sh -hostid host3 -clusterid cluster1 -port 1101 -config conf/gondola-remote.conf start 2>&1
}

function stop {
  jps | grep GondolaCommand | awk '{print $1}' | xargs kill
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
