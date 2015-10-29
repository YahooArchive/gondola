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

## Known Issues and To Dos

* Occassionally leader election with more than 2-nodes will take more cycles than it should to complete.
* Occassionally a unit test case will miss a tick and fail.
* The reliability test (Tsunami) can run for about 8 hours before it hits a known hard-to-reproduce bug.
* Documentation work is about 50% complete.
* There is a bottleneck in CoreMember.java. Removing that bottleneck could reduce latency by 30%.
* The performance test needs to be made easier to run.
* The commit() method is currently synchronous; should support a version that is asynchronous.
* The storage interface should be enhanced to support batch reads and writes to improve performance further,
especially during backfilling.

## Build

The package is built with the following command:
```
mvn package
```
The command will also run all the unit tests to make sure the package is correctly built.
If all is well, you should see output that looks like:
```
...
Tests run: 34, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 29.733 sec

Results :

Tests run: 34, Failures: 0, Errors: 0, Skipped: 0

[INFO] 
[INFO] --- appassembler-maven-plugin:1.9:assemble (default) @ gondola ---
[INFO] Installing artifact /Users/patc/.m2/repository/org/slf4j/slf4j-api/1.7.10/slf4j-api-1.7.10.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/slf4j-api-1.7.10.jar
[INFO] Installing artifact /Users/patc/.m2/repository/org/slf4j/slf4j-log4j12/1.7.10/slf4j-log4j12-1.7.10.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/slf4j-log4j12-1.7.10.jar
[INFO] Installing artifact /Users/patc/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/log4j-1.2.17.jar
[INFO] Installing artifact /Users/patc/.m2/repository/com/typesafe/config/1.2.1/config-1.2.1.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/config-1.2.1.jar
[INFO] Installing artifact /Users/patc/.m2/repository/mysql/mysql-connector-java/5.1.35/mysql-connector-java-5.1.35.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/mysql-connector-java-5.1.35.jar
[INFO] Installing artifact /Users/patc/.m2/repository/com/h2database/h2/1.4.187/h2-1.4.187.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/h2-1.4.187.jar
[INFO] Installing artifact /Users/patc/.m2/repository/org/apache/commons/commons-exec/1.3/commons-exec-1.3.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/commons-exec-1.3.jar
[INFO] Installing artifact /Users/patc/.m2/repository/commons-cli/commons-cli/1.3/commons-cli-1.3.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/commons-cli-1.3.jar
[INFO] Installing artifact /Users/patc/.m2/repository/com/zaxxer/HikariCP/2.3.8/HikariCP-2.3.8.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/HikariCP-2.3.8.jar
[INFO] Installing artifact /Users/patc/.m2/repository/org/javassist/javassist/3.18.2-GA/javassist-3.18.2-GA.jar to /Users/patc/projects/oss-gondola/target/appassembler/repo/javassist-3.18.2-GA.jar
[INFO] Installing artifact /Users/patc/projects/oss-gondola/target/classes to /Users/patc/projects/oss-gondola/target/appassembler/repo/gondola-0.1-SNAPSHOT.jar
[INFO] 
[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ gondola ---
[INFO] Building jar: /Users/patc/projects/oss-gondola/target/gondola-0.1-SNAPSHOT.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 34.550s
[INFO] Finished at: Sun Sep 27 10:10:47 PDT 2015
[INFO] Final Memory: 18M/442M
[INFO] ------------------------------------------------------------------------
```

## Unit Tests

The unit tests are located in ```src/test/java/com/yahoo/gondola/core/GondolaTest.java```.
To run the unit tests without having to build the package:
```
mvn test
```

To run a single test, for example, the test called 'missingEntry':
```
mvn test -Dtest=GondolaTest#missingEntry
```

## Tsunami Test

This stress test is designed to find bugs in the raft implementation. Here's how it works:

1. The test creates N writer threads for each node in the Raft cluster.
2. A test thread has an id and a private counter and attempts to write its thread id and counter value to its assigned node. When a write succeeds, the counter is incremented. The threads assigned to the current leader will be able to write; threads assigned to followers will fail to write.
3. A single reader thread reads Raft entries, one-at-a-time, from each node in round-robin fashion and ensures that the entries are identical.
4. The reader thread also maintains a hash of all thread ids and ensures that the written counter values are monotonically increasing for each of the write threads.
5. A restarter thread randomly and continuously restarts nodes to simulate random server failures.
6. The Tsunami storage implementation induces random failures on all storage operations.
7. The Tsunami network implementation generates random read, write, and connection failures on all networking operations. The wrapper will also delay and duplicate sent messages.


#### Running Tsunami

To run the Tsunami test you will need access to a MySQL database. Edit ```conf/gondola-tsunami.conf``` and
set the fields - url, user, password - with the correct credentials to your database.
```
storage_mysql {
  class = com.yahoo.gondola.impl.MySqlStorage
  url = "jdbc:mysql://127.0.0.1:3306/gondola"
  user = root
  password = root
}
```
The test will automatically create the necessary tables if it doesn't find them.

In one console, run the gondola agent command. This process listens for commands to start and stop a gondola instance.
```
> ./bin/gondola-agent.sh 
2015-09-27 10:07:58,638 INFO Initialize system, kill all gondola processes
2015-09-27 10:07:59,973 INFO Listening on port 1200
...
```

In another console, run the tsunami test:
```
> ./bin/tsunami.sh
Dropping all tables
2015-09-27 10:20:00,879 INFO Connecting to gondola agent at localhost:1200
2015-09-27 10:20:00,881 INFO Connecting to gondola agent at localhost:1200
2015-09-27 10:20:00,882 INFO Connecting to gondola agent at localhost:1200
2015-09-27 10:20:00,968 INFO writes: 0, reads: 0, errors: 0, waiting for index=0 on host1
2015-09-27 10:20:00,970 INFO Creating instance host1
2015-09-27 10:20:00,980 INFO   host1: up=false, host2: up=false, host3: up=false
2015-09-27 10:20:00,980 INFO   lastWrite: 0, lastRead: 0
2015-09-27 10:20:01,013 INFO Creating instance host2
2015-09-27 10:20:01,018 INFO Creating instance host3
2015-09-27 10:20:06,050 INFO --- SAFE phase ---
2015-09-27 10:20:09,612 INFO --- NASTY phase ---
2015-09-27 10:20:10,980 INFO writes: 80, reads: 79, errors: 30, waiting for index=80 on host2
2015-09-27 10:20:10,981 INFO   host1: up=true, host2: up=true, host3: up=true
2015-09-27 10:20:10,981 INFO   lastWrite: 80, lastRead: 79
2015-09-27 10:20:13,316 INFO Killing instance host2
2015-09-27 10:20:13,694 ERROR EOF: Failed to execute: g 124 30000
2015-09-27 10:20:13,697 ERROR EOF: Failed to execute: g 124 30000
2015-09-27 10:20:18,274 INFO Killing instance host3
2015-09-27 10:20:19,325 INFO Failed to write A2-45: [localhost:1099] c A2-45 -> ERROR: Not leader - leader is null
2015-09-27 10:20:19,325 INFO Failed to write A0-48: [localhost:1099] c A0-48 -> ERROR: Not leader - leader is null
2015-09-27 10:20:19,325 INFO Failed to write A4-47: [localhost:1099] c A4-47 -> ERROR: Not leader - leader is null
2015-09-27 10:20:19,325 INFO Failed to write A1-49: [localhost:1099] c A1-49 -> ERROR: Not leader - leader is null
2015-09-27 10:20:19,326 INFO Failed to write A3-41: [localhost:1099] c A3-41 -> ERROR: Not leader - leader is null
2015-09-27 10:20:20,413 INFO Creating instance host2
2015-09-27 10:20:20,985 INFO writes: 225, reads: 123, errors: 82, waiting for index=124 on host2
2015-09-27 10:20:20,985 INFO   host1: up=true, host2: up=true, host3: up=false
2015-09-27 10:20:20,985 INFO   lastWrite: 225, lastRead: 123
2015-09-27 10:20:23,192 ERROR EOF: Failed to execute: g 124 30000
2015-09-27 10:20:27,938 INFO Creating instance host3
2015-09-27 10:20:30,986 INFO writes: 246, reads: 214, errors: 128, waiting for index=215 on host2
2015-09-27 10:20:30,987 INFO   host1: up=true, host2: up=true, host3: up=true
2015-09-27 10:20:30,987 INFO   lastWrite: 252, lastRead: 214
2015-09-27 10:20:31,033 INFO Killing instance host3
2015-09-27 10:20:32,396 INFO Creating instance host3
2015-09-27 10:20:33,485 ERROR [localhost:1099] g 229 30000 -> ERROR: Unhandled exception: Nasty exception for index=229
2015-09-27 10:20:33,901 INFO Killing instance host2
2015-09-27 10:20:35,504 WARN Command at index 232 for writer A0=48 is a duplicate
2015-09-27 10:20:35,508 WARN Command at index 233 for writer A2=45 is a duplicate
2015-09-27 10:20:35,511 WARN Command at index 234 for writer A1=49 is a duplicate
2015-09-27 10:20:35,515 WARN Command at index 235 for writer A3=41 is a duplicate
2015-09-27 10:20:35,518 WARN Command at index 236 for writer A4=47 is a duplicate
2015-09-27 10:20:40,990 INFO writes: 444, reads: 449, errors: 194, waiting for index=450 on host2
2015-09-27 10:20:40,991 INFO   host1: up=true, host2: up=false, host3: up=true
2015-09-27 10:20:40,991 INFO   lastWrite: 450, lastRead: 449
2015-09-27 10:20:42,429 INFO Creating instance host2
2015-09-27 10:20:50,991 INFO writes: 640, reads: 645, errors: 259, waiting for index=646 on host2
2015-09-27 10:20:50,991 INFO   host1: up=true, host2: up=true, host3: up=true
2015-09-27 10:20:50,992 INFO   lastWrite: 646, lastRead: 645
2015-09-27 10:20:56,000 INFO Killing instance host2
2015-09-27 10:20:58,709 INFO Creating instance host2
2015-09-27 10:21:00,994 INFO writes: 830, reads: 835, errors: 329, waiting for index=836 on host2
2015-09-27 10:21:00,995 INFO   host1: up=true, host2: up=true, host3: up=true
2015-09-27 10:21:00,995 INFO   lastWrite: 836, lastRead: 835
2015-09-27 10:21:01,102 ERROR [localhost:1099] g 838 30000 -> ERROR: Unhandled exception: Nasty exception for index=838
2015-09-27 10:21:02,103 ERROR EOF: Failed to execute: g 838 30000
2015-09-27 10:21:10,584 INFO --- SYNC phase ---
2015-09-27 10:21:10,995 INFO writes: 1024, reads: 1030, errors: 396, waiting for index=1031 on host1
2015-09-27 10:21:10,995 INFO   host1: up=true, host2: up=true, host3: up=true
2015-09-27 10:21:10,995 INFO   lastWrite: 1030, lastRead: 1030
2015-09-27 10:21:12,792 INFO Creating instance host3
2015-09-27 10:21:12,796 INFO --- SAFE phase ---
...
2015-09-27 06:33:42,210 ERROR Command index=270742 from host1=A1-19118 does not match command from host2=A0-19014
> 
```
Run the tsunimi test for as long as you can, days if possible. It will continously try to find an error in the Raft implementation. As it runs, it periodically prints out its status:
```
writes: 640, reads: 645, errors: 259...
```
As long as the number of write and reads continue to increase, the test is running properly.

If tsunami does find an issue, the test will exit and print one of two types of errors:

* Command at index X...does not match...": This error indicates that
  not all hosts are returning the same value at index X.

* Command at index X for writer...is not one bigger than last index:
  This error indicates that the counter written by a writer is not
  equal to or one larger than it's last written value. Either a value
  was lost or an older value reappeared.


## Performance

TODO

## Usage

#### Terminology

| Term    | Description |
|:--------|-------------|
| Member  | A member refers a Raft node, which can be a follower, leader, or candidate. A member must be manually assigned a unique id in the config file. |
| Cluster | A cluster is a collection of members, only one of which can be the leader. A member can be only part of a one cluster. A cluster must be manually assigned a unique id in the config file. |
| Host    | Refers to a machine with an IP address and a port. A host can contain one or more clusters. All members are assigned a cluster and a primary host. All members running in the same host will share one port when communicating with the other memembers in the cluster. Hosts must be manually assigned a unique id in the config file. |
| Config | The entire topology of all clusters in a system is defined in a single config file. At the moment, dynamic reconfiguration of the topology is not supported. |
| Log Table | Each member has a logical log in which it writes all records to be committed. In Gondola, the log table contains a member_id column, which allows many members to share the same storage instance. At the moment, all logs are stored in a single database instance. We intend to enhance this so that each host can use a different database instance if needed. This means that all members running in a host will still share the same database instance.  |
| Gondola Instance | is a process that is running all the members residing in a host. The gondola instance provides a single port through which all the members in the instance can communicate with any other member on any other host. The gondola instance also provides a storage instance for all its members logs. |

#### Code

The first step in using Gondola is to copy the config file
```conf/gondola-sample.conf```. It contains a sample of a host/cluster
topology that you can modify to hold your topology. 
From this config file, you can create a Gondola instance.
The following are the lines of code to commit a command to the Raft log:
```
Config config = new Config(new File(configFile));
Gondola gondola = new Gondola(config, hostId);
Cluster cluster = gondola.getCluster(clusterId);
Command command = cluster.checkoutCommand();
command.commit(bytes, offset, len);
```

The following lines of code is used to retrieve committed commands from the Raft log:
```
Cluster cluster = gondola.getCluster(clusterId);
Command command = cluster.getCommittedCommand(index); // index starting from 1
String value = command.getString();
```

## Architecture

These are the major classes in the implementation:

| Class   | Description |
|:--------|-------------|
| Client  | Represents the client code. It is assumed that the client has many threads writing to the Raft log. |
| Cluster | Represents a set of Raft nodes, only one of which can be a leader. The Cluster object also has a pool of Command object which are used for committing and reading Raft commands. The Command objects are pooled to avoid needless garbage collection. |
| Config | Is an interface that provides access to all configuration information. The Config implementation can be a file or an adapter retrieving configs from some storage. |
| Connection | Represents a channel of communication between the local and remote member. |
| Exchange | Is reponsible for creating a Connection between the local and remote member. |
| Gondola | The Gondola instance that represents all the nodes in a particular host.
| Member | Represents a Raft node. All commands that are written by the client are placed in the CommandQueue. When the commands are processed from the CommandQueue, they are processed in as large as a batch that will fit into a Raft message. The command is assigned a Raft index. After the message is sent to the other members, the processed Command objects are moved to the WaitQueue, blocking the client until the Command's Raft index has been committed. The IncomingQueue holds Raft messages that need to be processed. |
| Message Pool | All Raft messages are represented by Message objects. The Message objects are pooled to avoid garbage collection. Message objects are reference counted, so they can be added to more than one outgoing queue for delivery to a remote member. |
| Peer | Represents remote members that this host needs to communicate with. The peer is responsible for sending and receiving messages from the remote member. The Peer is also responsible for backfilling the remote member if necessary. |
| SaveQueue | Is responsible for persisting messages in the SaveQueue to storage. The SaveQueue employs threads to allow inserts to happen concurrently. |
| Storage | Is an interface that can be implemented by any storage system that can provide strong consistency. |

The following is a rough class diagram of the major classes showing the major relationships between the classes:

![Design Diagram](https://raw.githubusercontent.com/yahoo/gondola/master/core/src/main/resources/gondola_design.png)
[source](https://docs.google.com/drawings/d/1DZwNXhH3iycqqBMFVulvyXVtIOar7h_USkMBdfTAWU4/edit)


