* Tsunami Test

This test creates a cluster of 3 members and randomly restarts the
members. Each member has 5 threads that are continuously writing to
the member. These writes succeed only when the member is the
leader. Otherwise, the writes fail.  Whenever a write fails, it is
retried.

The writers writes commands of the form:

```
MT-N

- M is the identity of member - A, B, or C.
- T is the identity of the thread - an integer from 1 to 5.
- N is a integer that is incremented by 1 after every successful write.
```

For example:

```
A1-33
A0-25
A2-43
A1-34
A0-25
A2-44
```

Note that it's possible for commands to be duplicated.


A reader reads every committed command and verifies that the sequences
are correct.  The reader reads the command at index 1 from all three
members and verifies that all commands are the same.  Then it tries
the next index and continues ad infinitem.

To run the test:

```
1. gondola-agent.sh
2. ./bin/mysql_server.sh drop
3. mvn -DskipTests test
```

