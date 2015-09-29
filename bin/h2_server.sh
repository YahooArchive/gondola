#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

for var in "$@"
do
  if [ "$var" == "start" ]
  then
    java -jar ./target/appassembler/repo/h2-1.4.187.jar -webAllowOthers -tcpAllowOthers
    exit
  fi

  if [ "$var" == "drop" ]
  then
    echo "Dropping all tables"
    java -cp ./target/appassembler/repo/h2-1.4.187.jar org.h2.tools.RunScript -script $DIR/h2_drop_tables.sql -url jdbc:h2:tcp://localhost/~/test -user sa -password ''
    exit
  fi
done
