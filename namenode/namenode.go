package namenode

import (
	"context"
	"log"
	"net"

	namenode "github.com/Raghav-Tiruvallur/GoDFS/proto/namenode"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

type DataNodeMetadata struct {
	Port   string
	Status string
}
type NameNodeData struct {
	BlockSize                 int64
	DataNodeToBlockMapping    map[string][]string
	ReplicationFactor         int64
	DataNodeToMetadataMapping map[string]DataNodeMetadata
	namenode.UnimplementedNamenodeServiceServer
}

func (nameNode *NameNodeData) InitializeNameNode(port string, blockSize int64) {

	nameNode.BlockSize = blockSize
	nameNode.DataNodeToBlockMapping = make(map[string][]string)
	nameNode.DataNodeToMetadataMapping = make(map[string]DataNodeMetadata)
	nameNode.ReplicationFactor = 3
	server := grpc.NewServer()
	namenode.RegisterNamenodeServiceServer(server, nameNode)
	address := ":" + port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Namenode is listening on port %s\n", address)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (nameNode *NameNodeData) Register_DataNode(ctx context.Context, datanodeData *namenode.DatanodeData) (status *namenode.Status, err error) {

	log.Printf("%s %d\n", datanodeData.DatanodeID, nameNode.BlockSize)
	_, ok := nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID]
	if !ok {
		nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID] = make([]string, 0)
		dnmetadata := DataNodeMetadata{Port: datanodeData.DatanodePort, Status: "Available"}
		nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID] = dnmetadata
		return &namenode.Status{StatusMessage: "Registered"}, nil
	}
	return &namenode.Status{StatusMessage: "Exists"}, nil

}

func (nameNode *NameNodeData) GetAvailableDatanodes(ctx context.Context, empty *empty.Empty) (freeNodes *namenode.FreeDataNodes, err error) {
	availableDataNodes := make([]*namenode.DatanodeData, 0)

	for dataNodeID, datanodeMetadata := range nameNode.DataNodeToMetadataMapping {
		if datanodeMetadata.Status == "Available" {
			datanodeData := &namenode.DatanodeData{DatanodeID: dataNodeID, DatanodePort: nameNode.DataNodeToMetadataMapping[dataNodeID].Port}
			availableDataNodes = append(availableDataNodes, datanodeData)
		}
	}

	return &namenode.FreeDataNodes{DataNodeIDs: availableDataNodes}, nil

}
