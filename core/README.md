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

This fuzz test is designed to find concurrency bugs in the Raft implementation. Here's how it works:

1. The test creates N writer threads for each node in the Raft cluster.
2. A writer thread has an id and a private counter and attempts to write its thread id and counter value to its assigned node. When a write succeeds, the counter is incremented. Writes by threads assigned to the current leader are expected to succeed whereas writes by threads threads assigned to non-leaders are expected to get exceptions.
3. A single reader thread reads Raft entries, one-at-a-time, from each node in round-robin fashion and ensures that the entries are identical.
4. The reader also records the counter values for every writer and checks that the latest counter values are monotonically increasing.
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
> ./bin/gondola-agent
2015-09-27 10:07:58,638 INFO Initialize system, kill all gondola processes
2015-09-27 10:07:59,973 INFO Listening on port 1200
...
```

In another console, run the tsunami test:
```
> ./bin/tsunami
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

#### Code

The first step in using Gondola is to copy the config file
```conf/gondola-sample.conf```. It contains a sample of a host/shard
topology that you can modify to hold your topology.
From this config file, you can create a Gondola instance.
The following are the lines of code to commit a command to the Raft log:
```
Config config = new Config(new File(configFile));
Gondola gondola = new Gondola(config, hostId);
Shard shard = gondola.getShard(shardId);
Command command = shard.checkoutCommand();
command.commit(bytes, offset, len);
```

The following lines of code is used to retrieve committed commands from the Raft log:
```
Shard shard = gondola.getCluster(shardId);
Command command = shard.getCommittedCommand(index); // index starting from 1
String value = command.getString();
```

## Architecture

These are the major classes in the implementation:

| Class   | Description |
|:--------|-------------|
| Client  | Represents the client code. It is assumed that the client has many threads writing to the Raft log. |
| Shard | Represents a set of Raft nodes, only one of which can be a leader. The Shard object also has a pool of Command object which are used for committing and reading Raft commands. The Command objects are pooled to avoid needless garbage collection. |
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
