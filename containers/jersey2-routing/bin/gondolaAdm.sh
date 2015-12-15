#!/usr/bin/env bash

appUri="http://localhost:8080/api/gondola/v1"
CURL="curl -v "
case $1 in
  "enableHost")
  $CURL -d " " "$appUri/enableHost?hostId=$2&enable=$3"
  ;;
  "enableSite")
  $CURL -d " " "$appUri/enableSite?siteId=$2&enable=$3"
  ;;
  "assignBuckets")
  $CURL -d " " "$appUri/assignBuckets?lowerBound=$2&upperBound=$3&fromShardId=$4&toShardId=$5"
  ;;
  "setSlave")
  $CURL -d " " "$appUri/setSlave?shardId=$2&masterShardId=$3"
  ;;
  "unsetSlave")
  $CURL -d " " "$appUri/unsetSlave?shardId=$2&masterShardId=$3"
  ;;
  "setLeader")
  $CURL -d " " "$appUri/setLeader?hostId=$2&shardId=$3"
  ;;
  "hostStatus")
  $CURL "$appUri/hostStatus?hostId=$2"
  ;;
  "serviceStatus")
  $CURL "$appUri/serviceStatus"
  ;;
esac
