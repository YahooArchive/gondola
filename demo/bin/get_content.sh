#!/bin/sh

if [ $# != 1 ]; then
  echo "Get content from web service";
  echo "";
  echo "Usage: $0 key";
  exit;
fi

ports="8080 8081 8082"
for port  in $ports; do
  echo "port: $port : "
  curl localhost:$port/api/entries/$1 -H "ContentType: application/json"
  echo ""
done
