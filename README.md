[![Build Status](https://travis-ci.org/yahoo/gondola.svg?branch=master)](https://travis-ci.org/yahoo/gondola)
[![Coverage Status](https://coveralls.io/repos/yahoo/gondola/badge.svg?branch=master&service=github)](https://coveralls.io/github/yahoo/gondola?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.yahoo.gondola/gondola-main/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.yahoo.gondola/gondola-main/)
[![Dependency Status](https://www.versioneye.com/user/projects/56563c28ff016c002c001c4c/badge.svg?style=flat)](https://www.versioneye.com/user/projects/56563c28ff016c002c001c4c)

# High-performance Raft-based Java Web Container

Gondola is a high-performance implementation of the [Raft
protocol](https://raft.github.io) written in Java.  Gondola also
includes support for building a complete scaleable and fault-tolerant
webservice, all based on the strongly consistent Raft distributed log.

The current Gondola implementation is robust enough to run performance
experiments but not yet ready for production. We have plans to use
this package in production and will be making improvements toward that
goal. We are releasing this package earlier in the hopes of getting
feedback on how we might make it more useful to more use cases. Of
course pull requests are especially welcome.

## Features

* Replaceable storage for the Raft log. Implementations for MySQL and H2 are available.
* Pipelining support. All Raft messages are sent asynchronous to minimize latency.
* Batching support. Improves throughput under heavy load or slower networks.
* Zero-allocation implementation. The implementation reuses objects whereever possible to minimize garbage collection issues (unfortunately database drivers still create objects).
* Pre-vote support. A Raft optimization that avoids unnecessary elections when a node joins or re-joins the cluster.
* Configuration support for multiple data centers.
* The container supports sharding, automatic request routing to leaders, dynamic membership with non-static IPs, archiving.

## Demo

This demo uses the Gondola container to implement a simple,
fault-tolerant web service that provides a strongly consistent
key/value store (which means that when a client writes a value, any
subsequent reads by that client will return that same value).  The
service supports a RESTful API for setting and retrieving values.  The
demo starts up three servers, each using their own H2DB instance to
implement the Raft log.

#### Start the Demo Servers

Run the following commands in three different consoles on the same
machine. Each will start up one of the nodes in the three-node
cluster. The servers will elect a leader and will print out their
current role:

```
> cd examples/kv-server
> bin/run host1
...
INFO [host1] Current role: CANDIDATE
INFO [host1] Current role: LEADER
INFO [host1] Ready
```

```
> cd examples/kv-server
> bin/run host2
...
INFO [host2] Current role: CANDIDATE
INFO [host2] Current role: FOLLOWER
```

```
> cd examples/kv-server
> bin/run host3
...
INFO [host3] Current role: CANDIDATE
INFO [host3] Current role: FOLLOWER
```

#### Testing the Demo Servers

In yet another console, run commands to set and retrieve values. You
can send commands to any host and the host will automatically route
requests to the leader.

```
> bin/put_key host1 Taiwan Taipei
> bin/put_key host2 Canada Ottawa
> bin/get_key host3 Taiwan
Taipei
```

Here's what you would see in the leader's console:
```
INFO [host1] Current role: CANDIDATE
INFO [host1] Current role: LEADER
INFO [host1] Ready
INFO [host1] Put key Taiwan=Taipei
INFO [host1] Executing command 1: Taiwan Taipei
INFO [host1] Put key Canada=Ottawa
INFO [host1] Executing command 2: Canada Ottawa
INFO [host1] Get key Taiwan: Taipei
```

If you kill the leader, one of the other followers will become the
leader and start serving requests:

```
INFO [host3] Current role: CANDIDATE
INFO [host3] Current role: LEADER
INFO [host3] Executing command 3: 
INFO [host3] Ready
INFO [host3] Put key USA=Washington, D.C.
INFO [host3] Executing command 4: USA Washington, D.C.
```

Note blank commands (command 3) are written into the log after a
leader election.  This is due to a Raft requirement that entries are
not considered committed unless the last committed entry is of the
current term. Applications need to ignore these blank commands.

## Gondola Terminology

These are the terms used through the libraries and tools in the Gondola package:

| Term      | Description |
|:----------|-------------|
| Member    | A member refers a Raft node, which can be a follower, leader, or candidate. Member ids can be a statically or dynamically assigned.
| Shard     | A shard is a set of members, only one of which can be the leader. A member can be only part of a one shard. Shards have a manually assigned id.
| Host      | Refers to a machine with an IP address and a port. A host can run members from one or more shards. All members are assigned a shard and a primary host. All members running in the same host will share one port when communicating with the other memembers in the shard. Host ids can be statically or dynamically assigned. |
| Site      | Refers to a set of hosts. A site can be roughly thought of as a data center. A host can only be in a single site. A site has a manually assigned id.
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

* The reliability test (Tsunami) can run for about 8 hours before it hits a known hard-to-reproduce bug.
* Documentation needs to be improved.
* CoreMember.java has a known bottleneck. Removing that bottleneck could reduce latency by 30%.
* The performance test needs to be made easier to run.
* The commit() method is currently synchronous; an asynchronous version should be supported.
* The storage interface should be enhanced to support batch reads and writes to improve performance further,
especially during backfilling.
* Container needs to support re-sharding.
* Handling dynamic IPs is WIP.
* Need to implement leader affinity - where in a shard, a leader prefers to be running in a particular host if there are no issues.
* Support for each site to have it's own database instance.
* Authentication for the Gondola port (the one where Raft messages are exchanged).

