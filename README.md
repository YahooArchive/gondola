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

A simple fault-tolerant service for storing key/value pairs has been built to demonstrate Gondola capabilities.


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

## Known Issues and To Dos

* Occassionally leader election with more than 2-nodes will take more cycles than it should to complete.
* The reliability test (Tsunami) can run for about 8 hours before it hits a known hard-to-reproduce bug.
* Documentation needs to be improved quite a bit.
* CoreMember.java has a known bottleneck. Removing that bottleneck could reduce latency by 30%.
* The performance test needs to be made easier to run.
* The commit() method is currently synchronous; an asynchronous version should be supported.
* The storage interface should be enhanced to support batch reads and writes to improve performance further,
especially during backfilling.

