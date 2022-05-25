# README #

Distributed log

### Distributed log ###

* A distributed log written in Go. Uses raft-algorithm for data-replication, and gRPC for communication.
* Version 1.0.0

### Setup ###

#### Compile gRPC clients and servers from protobuf
 $ make compile
#### Generate TLS certificates
 $ make gencert
#### Build docker image
 $ make build-docker
#### Run tests
 $ make test
