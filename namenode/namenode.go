package namenode

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	goDFS "github.com/Raghav-Tiruvallur/GoDFS/proto"
	"github.com/Raghav-Tiruvallur/GoDFS/utils"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type NameNodeData struct {
	BlockSize              int64
	DataNodeToBlockMapping map[string][]string
	//goDFS.UnimplementedGODFSServer
}

func (nameNode *NameNodeData) InitializeNameNode(port string, blockSize int64) {

	server := grpc.NewServer()
	ms := &NameNodeData{}
	nameNode.BlockSize = blockSize
	goDFS.RegisterGODFSServer(server, ms)
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

func (namenode *NameNodeData) WriteFile(sourcePath string, fileName string) {

	filePath := filepath.Join(sourcePath, fileName)

	//read from file
	//get block size from namenode
	blockSize := int(3 * 1024)
	fileSizeHandler, err := os.Stat(filePath)

	utils.ErrorHandler(err)

	fileSize := int(fileSizeHandler.Size())

	numberOfBlocks := fileSize / blockSize

	if fileSize%blockSize > 0 {
		numberOfBlocks++
	}

	buffer := make([]byte, blockSize)

	fileHandler, err := os.Open(filePath)

	utils.ErrorHandler(err)

	for i := 0; i < numberOfBlocks; i++ {
		_, err := fileHandler.Read(buffer)
		if err == io.EOF {
			break
		}
		utils.ErrorHandler(err)
		blockID := uuid.New().String()
		freeDataNodes, err := namenode.GetAvailableDatanodes(context.Background(), &emptypb.Empty{})
		//freeDataNodes := [4]string{"1", "2", "3", "4"}
		utils.ErrorHandler(err)
		log.Println(blockID)
		for _, datanode := range freeDataNodes.DataNodeIDs {
			// clientDataNodeRequest := goDFS.ClientToDataNodeRequest{BlockID: blockID, Content: buffer[:n]}
			// client.SendDataToDataNodes(context.Background(), &clientDataNodeRequest)
			log.Println(datanode)
		}
	}

	//get available datanodes from namenode
	//break the file into many blocks
	//send each block to a datanode
	//datanode then takes care of replicating it across other datanode

}
func (namenode *NameNodeData) GetAvailableDatanodes(ctx context.Context, empty *emptypb.Empty) {

}
