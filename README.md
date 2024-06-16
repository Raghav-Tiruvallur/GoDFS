#  GoDFS

    A distributed file system inspired by HDFS built in Golang and gRPC. It is designed to provide high availability, scalability, and fault tolerance for file storage across multiple nodes. GoDFS uses gRPC for fast and efficient communication between nodes.

## Features
- **Distributed Architecture**: Store files across multiple nodes to ensure high availability and redundancy.
- **Fault Tolerance**: Automatically replicate files across nodes to prevent data loss in case of node failures.
- **High Performance**: Optimized for fast file operations and low latency using gRPC for communication

## Components
There are three main components of GoDFS:
- **Namenode** : Responsible for storing all the metadata of the files in GoDFS and acts as the brain of the system
- **Datanode** : Responsible for storing the file data in chunks and forwarding chunks to other datanodes
- **Client** : Responsible for handling the read and write requests for files

## Running the application

Before running the applications, you have to generate .go files from .proto files : `make protoc`


- Run the namenode: `make run-namenode`
- Run the datanodes: `make run-datanodes`
- Run the client write: `make run-client-write`
- Run the client read:  `make run-client-read`





