# RaftFS

## Raft Overview 

The [Raft](https://raft.github.io/) consensus algorithm is a simplified alternative to the popular Paxos consensus algorithm. It has been proposed in 2014 and you can read the original paper [here](https://raft.github.io/raft.pdf).

Because of the [CAP](https://en.wikipedia.org/wiki/CAP_theorem) theorem, Raft gives up on availability in order to guarantee consistency and partition tolerance (as for Paxos). In addition, it implments a strong consistency, i.e., the system behaviour (apparent order and visibility of updates) is equivalent to a single-machine system. In particular, Raft implements a [linearizable consistency model](http://www.bailis.org/blog/linearizability-versus-serializability/).

## RaftFS

RaftFS is a Java implementation of the Raft alogrithm to deploy  a file system on distributed environments. 
It has been developed for the final term project of the [Distributed Enabling Platform](http://didawiki.cli.di.unipi.it/doku.php/magistraleinformaticanetworking/cpa/start) course of the University of Pisa. 
The main goal of this project was not to provide a distributed file system to manage big quantity of data, but to implement a strong consistent algorithm and
try to apply it to some kind of general-purpose application (like a file system indeed). For more details about this project, look at the report file in this repository.

## System Requirements

This project has been tested and developed by using:

* Java 1.8.0
* [Maven](https://maven.apache.org/) 3.3.9
* [Vagrant](https://www.vagrantup.com/) 1.9.8

In addition, the following libaries are used in this project (and managed by Maven):

* [Apache log4j](https://logging.apache.org/log4j/2.x/) 1.2.17
* [Apache Commons Net](https://commons.apache.org/proper/commons-net/) 3.3
* [Apache FtpServer](https://mina.apache.org/ftpserver-project/) 1.0.6
* [Google Guava](https://github.com/google/guava) 18.0
* [snakeyaml](https://bitbucket.org/asomov/snakeyaml) 1.15

## User Manual

From now on, we suppose that the current directory is the `src` directory of this repository.

### Project Building
`mvn -f ./RaftFS/pom.xml package` to build RaftFS with Maven. 

If the process succeed, the directory `./RaftFS/target` should have been created and containing the jar file `RaftFS-1.0-SNAPSHOT-jar-
with-dependencies.jar`.

### Local Version

1. `java -cp RaftFS/target/RaftFS-1.0-SNAPSHOT-jar-with-dependencies.jar
PAD.RaftFS.Server.MainLocal n` to run a local version of RaftFS, where `n` is the number of servers. Each server
identifier is `ServerI` where `I=0...n-1`. Executing the instruction above without `n` prints the local version usage.
2. Now that a local version of RaftFS is running, for each node a directory should have been created
(containing the persistent fields files) and a directory `./ServerFS` containing a directory for each server: it will contains each file uploaded on that server.
3. Now, the user can interact with the RaftFS with several commands. The most relevant are:
    * `timeout ServerName`: the state of `ServerName` will be forcefully changed in Candidate and
so starting a new election with a new term.
    * `stop ServerName`: the server `ServerName` crashes (the executor is interrupted and the server
not reachable by RMI).
    * `resume ServerName`: reinitialize `ServerName` as a Follower. This operation is valid only if
`stop ServerName` was previously executed.
    * `put ServerName CommandPath LocalFile Quantity`: ask to `ServerName` to upload (which is the actual Leader) the local
file `LocalFile` (absolute path to a local file) on the file system with absolute path `CommandPath` on at most `Quantity` servers.
4. Several other commands are described in section 3.1.1 of the report file.

### Pseudo and Distributed Version

See section 3.1.2 of the report file.
