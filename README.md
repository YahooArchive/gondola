[![Build Status](https://travis-ci.org/yahoo/gondola.svg?branch=master)](https://travis-ci.org/yahoo/gondola)

# High-performance Java Implementation of Raft Protocol

Gondola is a high-performance implementation of the [Raft
protocol](https://raft.github.io) written in Java.  The current
implementation is robust enough to run performance experiments but not
yet ready for production. We have plans to use this package in
production and will be making improvements toward that goal. We are
releasing this package earlier in the hopes of getting feedback on how
we might make it more useful to more use cases. Of course pull
requests are especially welcome.

## Features

* Replaceable storage for the Raft log. Implementations for MySQL and H2 are available.
* Pipelining support. All Raft messages are sent asynchronous to minimize latency.
* Batching support. Improves throughput under heavy load or slower networks.
* Zero-allocation implementation. The implementation reuses objects whereever possible to minimize garbage collection issues (unfortunately database drivers still create objects).
* Pre-vote support. A Raft optimization that avoids unnecessary elections when a node joins or re-joins the cluster.

## Demo

This demo is an example that uses the Gondola library to build a
simple fault-tolerant web service that implements a strongly consistent
key/value store.  The demo starts up three servers, each implementing
a RESTful API for setting and getting values.  The demo uses H2DB as
the backing storage for the Raft log so that the updates are
persistant.

#### Start the Servers

Run the following commands in three different consoles in the same server. Each will
start up one of the nodes in the three node cluster.

```
[console 1]> cd examples/kv-server
[console 1]> bin/run host1

[console 2]> cd examples/kv-server
[console 2]> bin/run host2

[console 3]> cd examples/kv-server
[console 3]> bin/run host3
```

The servers will elect a leader and will print out their current role:

```
15:23:28.611 INFO - Current role: FOLLOWER
15:23:28.911 INFO - Current role: CANDIDATE
15:23:29.107 INFO - Current role: LEADER
```

#### Test

In yet another console, run commands to set and retrieve values:

```
[console 4]> put_key host1 key_1 value_1
[console 4]> put_key host2 key_2 value_2
[console 4]> get_key host1 key_2
[console 4]>  key2 value_2
```

If all is working correctly, sending commands to any server will route the request to the leader.
You can see the non-leader route the request to the leader and see the leader process the request.

```
```

You can continue testing the failover behavior by killing and restarting any of the three servers.

## Gondola Terminology

These are the terms used through the libraries and tools in the Gondola package:

| Term      | Description |
|:----------|-------------|
| Member    | A member refers a Raft node, which can be a follower, leader, or candidate. Members have a preassigned or dynamically assigned id.
| Shard     | A shard is a set of members, only one of which can be the leader. A member can be only part of a one shard. Shards have a manually assigned id.
| Host      | Refers to a machine with an IP address and a port. A host can run members from one or more shards. All members are assigned a shard and a primary host. All members running in the same host will share one port when communicating with the other memembers in the shard. Hosts have a preassigned or dynamically assigned id. |
| Site      | Refers to a set of hosts. A site can be roughly thought of as a data center. A host can only be in a one site. A site has a manually assigned id.
| Storage   | Refers to the storage system holding the raft log. Every site has at least one storage instance. A member writes it's Raft log entries into a single storage instance in the same site. A storage instance has a manually assigned id. |
| Config    | The entire topology of all clusters in a system is defined in a single config file. |
| Log Table | Each member has a logical log in which it writes all records to be committed. In Gondola, the log table contains a member_id column, which allows many members to share the same storage instance. At the moment, all logs are stored in a single database instance. We intend to enhance this so that each host can use a different database instance if needed. This means that all members running in a host will still share the same database instance.  |
| Gondola Instance | Is a process that is running all the members residing in a host. The gondola instance provides a single port through which all the members in the instance can communicate with any other member on any other host. |
| Gondola Core | Refers to the Java package that strictly implements the Raft protocol. See the [Gondola.java](core/src/main/java/com/yahoo/gondola/Gondola.java) interface. |
| Gondola Container | Refers to tools and libraries which help you build a complete web service based on the Gondola Core. It includes servlet filters that maintain a routing table and automatically routes requests to the leader. It contains management tools to help with adding shards. |
| Gondola Registry | Is a component of the Gondola Container package and refers to a process that supports service discovery as well as config management. |

## How to use

maven - pom.xml
```
  <dependencies>
  ...
        <dependency>
            <groupId>com.yahoo.gondola</groupId>
            <artifactId>core</artifactId>
            <version>0.2.7</version>
        </dependency>
  ...
    </dependencies>
```

## Building This Package

The package is built with the following command:
```
mvn package
```
The command will also run all the unit tests to make sure the package is correctly built.
If all is well, you should see output that looks like:
```
...
/com/yahoo/gondola/kvserver/0.2.8-SNAPSHOT/kvserver-0.2.8-SNAPSHOT-sources.jar
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO] 
[INFO] gondola ........................................... SUCCESS [3.340s]
[INFO] gondola-core ...................................... SUCCESS [4.162s]
[INFO] containers ........................................ SUCCESS [0.827s]
[INFO] gondola-container-jersey2-routing ................. SUCCESS [4.454s]
[INFO] registry .......................................... SUCCESS [2.694s]
[INFO] examples .......................................... SUCCESS [0.524s]
[INFO] gondola-example-kvserver .......................... SUCCESS [3.655s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

## Known Issues and To Dos

* Occassionally leader election with more than 2-nodes will take more cycles than it should to complete.
* The reliability test (Tsunami) can run for about 8 hours before it hits a known hard-to-reproduce bug.
* Documentation needs to be improved quite a bit.
* CoreMember.java has a known bottleneck. Removing that bottleneck could reduce latency by 30%.
* The performance test needs to be made easier to run.
* The commit() method is currently synchronous; an asynchronous version should be supported.
* The storage interface should be enhanced to support batch reads and writes to improve performance further,
especially during backfilling.

