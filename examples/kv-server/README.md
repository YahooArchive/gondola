# Key-Value Server Application

This demo application implements a fault-tolerant hash table using a
Raft cluster of three nodes.  The demo starts up three servers, each
implementing a RESTful API for setting and getting values.  The demo
uses H2DB as the backing storage for the Raft log so that the updates
are persistant.

## API

This API has two endpoints for retrieving and storing key-value pairs.

Get & Save entries:
  - GET /api/entries/{key}
  - PUT /api/entries/{key}
  
The entry is a simple string.

## Start the Servers

Run the following commands in three different consoles in the same server. Each will
start up one of the nodes in the three node cluster.
TODO: enhance the demo to work on different machines.

```
[console 1]> bin/run.sh host1

[console 2]> bin/run.sh host2

[console 3]> bin/run.sh host3
```

## Test

These shell scripts can be used to set and get values from the service.

### Set Content

The demo sets the server listening to port 8080 as the leader. The
put_contents.sh script is hard-coded to write to the leader. TODO:
enhance scripts to determine current leader and write to the current
leader.

```
> bin/put_content.sh
Set value to key via web service

Usage: bin/put_content.sh key value [host1|host2|host3]
> bin/put_content.sh 123 3456 host1
*   Trying ::1...
* Connected to localhost (::1) port 8080 (#0)
> PUT /api/entries/123 HTTP/1.1
> Host: localhost:8080
> User-Agent: curl/7.43.0
> Accept: */*
> Content-Type: application/json
> Content-Length: 28
>
* upload completely sent off: 28 out of 28 bytes
< HTTP/1.1 204 No Content
< Date: Tue, 29 Sep 2015 15:34:01 GMT
< Server: Jetty(9.3.0.M1)
<
* Connection #0 to host localhost left intact
```

### Get Content

This script will retrieve the specified key from all nodes in the cluster.

```
> ./bin/get_content.sh
Get content from web service

Usage: ./bin/get_content.sh key

> bin/get_content.sh 123
port: 8080 :
3456
port: 8081 :
3456
port: 8082 :
3456
```

## Failover Test
The folloing commands will demo the scenario:
  - host1 server down
  - host2 or host3 take over the leader
  - send write command to the new leader
  - get data from all servers
  - bring host1 back
  - get data from all servers

```
// Run three servers
[host1]> ./bin/run.sh host1
[host2]> ./bin/run.sh host2
[host3]> ./bin/run.sh host3

// put initial data to server 
[admin]> ./bin/put_content.sh fooKey fooValue

// Kill host1 server
[host1]> ^c

// host3 become a leader
[host3]>
...
2015-09-30 00:23:12 INFO  CoreMember:621 - [host3-83] Becomes LEADER for term 67
...

// Get key "fooKey" from all servers
[admin]> ./bin/get_content.sh fooKey
port: 8080 :
curl: (7) Failed to connect to localhost port 8080: Connection refused

port: 8081 :
fooValue
port: 8082 :
fooValue

// Set key "fooKey" with value on host3
[admin]> ./bin/put_content.sh fooKey fooValue2 host3
...
< HTTP/1.1 204 No Content

// get key "fooKey" again
[admin]> ./bin/get_content.sh fooKey
port: 8080 :
curl: (7) Failed to connect to localhost port 8080: Connection refused

port: 8081 :
fooValue2
port: 8082 :
fooValue2

// bring host1 back
[host1]> ./bin/run.sh host1

// get key "fooKey" again, host1 should get up-to-date value
[admin]> ./bin/get_content.sh 123
port: 8080 :
fooValue2
port: 8081 :
fooValue2
port: 8082 :
fooValue2

```
