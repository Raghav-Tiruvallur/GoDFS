package main

import (
	"flag"
	"log"
	"os"

	"github.com/Raghav-Tiruvallur/GoDFS/client"
	"github.com/Raghav-Tiruvallur/GoDFS/datanode"
	"github.com/Raghav-Tiruvallur/GoDFS/namenode"
)

func main() {

	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)

	dataNodePortPtr := dataNodeCommand.String("port", "8081", "Port of datanode")
	dataNodeLocationPtr := dataNodeCommand.String("location", ".", "Location of files to be stored by datanode")

	nameNodePortPtr := nameNodeCommand.String("port", "8080", "Port of namenode")
	nameNodeBlockSizePtr := nameNodeCommand.Int64("block-size", 32, "Block size to store")

	//clientPortPtr := clientCommand.String("port", "8080", "Port of client")
	clientNameNodePortPtr := clientCommand.String("namenode", *nameNodePortPtr, "NameNode communication port")
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")
	clientSourcePathPtr := clientCommand.String("source-path", ".", "Source path of the file")
	clientFilenamePtr := clientCommand.String("filename", "", "File name")

	if len(os.Args) < 2 {
		log.Println("sub-command is required")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		namenodePtr := &namenode.NameNodeData{}
		namenodePtr.InitializeNameNode(*nameNodePortPtr, *nameNodeBlockSizePtr)
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		datanodePtr := &datanode.DataNode{}
		datanodePtr.InitializeDataNode(*dataNodePortPtr, *dataNodeLocationPtr)
		conn := datanodePtr.ConnectToNameNode(*nameNodePortPtr, "localhost")
		datanodePtr.RegisterNode(conn)
		datanodePtr.StartServer(*dataNodePortPtr)
	case "client":
		_ = clientCommand.Parse(os.Args[2:])
		if *clientOperationPtr == "write" {
			clientPtr := &client.ClientData{}
			namenodePtr := &namenode.NameNodeData{}
			clientPtr.InitializeClient(*clientNameNodePortPtr)
			namenodePtr.WriteFile(*clientSourcePathPtr, *clientFilenamePtr)
		}

	}

}
