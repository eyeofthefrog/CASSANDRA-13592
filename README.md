# CASSANDRA-13991

This code will pull the Cassandra 3.11.1 docker image, start the container, and recreate an issue similar to https://issues.apache.org/jira/browse/CASSANDRA-13991

To run the example:
```
go get github.com/eyeofthefrog/CASSANDRA-13991
cd ~/go/src/github.com/eyeofthefrog/CASSANDRA-13991
go get ./...
go build
./CASSANDRA-13991
```