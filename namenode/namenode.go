package namenode

import (
	"context"
	"errors"
	"log"
	"net"
	"sort"

	namenode "github.com/Raghav-Tiruvallur/GoDFS/proto/namenode"
	"github.com/Raghav-Tiruvallur/GoDFS/utils"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

type DataNodeMetadata struct {
	ID     string
	Port   string
	Status string
}
type NameNodeData struct {
	BlockSize                 int64
	DataNodeToBlockMapping    map[string][]string
	ReplicationFactor         int64
	DataNodeToMetadataMapping map[string]DataNodeMetadata
	FileToBlockMapping        map[string][]string
	namenode.UnimplementedNamenodeServiceServer
}

type DataNodeBlockCount struct {
	DataNodeData *namenode.DatanodeData
	BlockCount   int64
}

func (nameNode *NameNodeData) InitializeNameNode(port string, blockSize int64) {

	nameNode.BlockSize = blockSize
	nameNode.DataNodeToBlockMapping = make(map[string][]string)
	nameNode.DataNodeToMetadataMapping = make(map[string]DataNodeMetadata)
	nameNode.FileToBlockMapping = make(map[string][]string)
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
		dnmetadata := DataNodeMetadata{ID: datanodeData.DatanodeID, Port: datanodeData.DatanodePort, Status: "Available"}
		nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID] = dnmetadata
		return &namenode.Status{StatusMessage: "Registered"}, nil
	}
	return &namenode.Status{StatusMessage: "Exists"}, nil

}

func (nameNode *NameNodeData) GetAvailableDatanodes(ctx context.Context, empty *empty.Empty) (freeNodes *namenode.FreeDataNodes, err error) {
	availableDataNodes := make([]*DataNodeBlockCount, 0)
	freeDataNodes := make([]*namenode.DatanodeData, 0)
	for dataNodeID, datanodeMetadata := range nameNode.DataNodeToMetadataMapping {
		if datanodeMetadata.Status == "Available" {
			datanodeData := &namenode.DatanodeData{DatanodeID: dataNodeID, DatanodePort: nameNode.DataNodeToMetadataMapping[dataNodeID].Port}
			blockCount := int64(len(nameNode.DataNodeToBlockMapping[dataNodeID]))
			dataNodeBlockCount := &DataNodeBlockCount{DataNodeData: datanodeData, BlockCount: blockCount}
			availableDataNodes = append(availableDataNodes, dataNodeBlockCount)
		}
	}

	sort.SliceStable(availableDataNodes, func(i, j int) bool {
		return availableDataNodes[i].BlockCount < availableDataNodes[j].BlockCount
	})
	for i := 0; i < int(nameNode.ReplicationFactor); i++ {
		freeDataNode := &namenode.DatanodeData{DatanodeID: availableDataNodes[i].DataNodeData.DatanodeID, DatanodePort: availableDataNodes[i].DataNodeData.DatanodePort}
		freeDataNodes = append(freeDataNodes, freeDataNode)
	}
	return &namenode.FreeDataNodes{DataNodeIDs: freeDataNodes[:nameNode.ReplicationFactor]}, nil

}

func (nameNode *NameNodeData) BlockReport(ctx context.Context, dataNodeBlockData *namenode.DatanodeBlockData) (status *namenode.Status, err error) {

	nameNode.DataNodeToBlockMapping[dataNodeBlockData.DatanodeID] = dataNodeBlockData.Blocks
	return &namenode.Status{StatusMessage: "Block Report Recieved"}, nil
}

func (nameNode *NameNodeData) FindDataNodesByBlock(blockID string) []DataNodeMetadata {

	dataNodes := make([]DataNodeMetadata, 0)

	for dataNode, blocks := range nameNode.DataNodeToBlockMapping {
		if utils.ValueInArray(blockID, blocks) {
			dataNodes = append(dataNodes, nameNode.DataNodeToMetadataMapping[dataNode])
		}

	}
	return dataNodes
}

func (nameNode *NameNodeData) GetDataNodesForFile(ctx context.Context, fileData *namenode.FileData) (*namenode.BlockData, error) {

	blocks, ok := nameNode.FileToBlockMapping[fileData.FileName]
	dataNodes := make([]*namenode.BlockDataNode, 0)
	if !ok {
		return nil, errors.New("file does not exist")
	}
	for _, block := range blocks {
		dataNodeList := nameNode.FindDataNodesByBlock(block)
		dataNodeIDsList := make([]*namenode.DatanodeData, 0)
		for _, datanode := range dataNodeList {
			dataNodeIDsList = append(dataNodeIDsList, &namenode.DatanodeData{DatanodeID: datanode.ID, DatanodePort: datanode.Port})
		}
		blockData := &namenode.BlockDataNode{BlockID: block, DataNodeIDs: dataNodeIDsList}
		dataNodes = append(dataNodes, blockData)
	}

	return &namenode.BlockData{BlockDataNodes: dataNodes}, nil

}

func (nameNode *NameNodeData) FileBlockMapping(ctx context.Context, fileBlockMetadata *namenode.FileBlockMetadata) (*namenode.Status, error) {

	filePath := fileBlockMetadata.FilePath
	blockIDs := fileBlockMetadata.BlockIDs

	nameNode.FileToBlockMapping[filePath] = blockIDs
	return &namenode.Status{StatusMessage: "Success"}, nil

}
