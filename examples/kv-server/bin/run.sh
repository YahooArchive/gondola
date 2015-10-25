#!/bin/bash
if [ $1 = "host1" ]; then
env hostId=host1 mvn jetty:run -Djetty.port=8080
elif [ $1 = "host2" ]; then
env hostId=host2 mvn jetty:run -Djetty.port=8081
elif [ $1 = "host3" ]; then
env hostId=host3 mvn jetty:run -Djetty.port=8082
else
  echo "Run service ";
  echo "";
  echo "Usage: $0 [host1|host2|host3]";
fi
