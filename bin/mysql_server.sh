#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

for var in "$@"
do
  if [ "$var" == "start" ]
  then
    mysqld
    exit
  fi

  if [ "$var" == "drop" ]
  then
    echo "Dropping all tables"
    mysql -u root -proot -h 127.0.0.1 gondola -e 'drop table if exists member_info, logs'
    exit
  fi
done
