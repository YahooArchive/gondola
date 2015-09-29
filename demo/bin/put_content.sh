#!/bin/sh

if [ $# < 2 ]; then
  echo "Set value to key via web service";
  echo "";
  echo "Usage: $0 key value [host1|host2|host3]";
  exit;
fi

if [ $3 = "host2" ]; then
  port=8081
elif [ $3 = "host3" ]; then
  port=8082
else
  port=8080
fi
curl -X PUT -d "{\"key\":\"$1\",\"value\":\"$2\"}" -H "Content-Type: application/json" -v localhost:$port/api/entries/$1
