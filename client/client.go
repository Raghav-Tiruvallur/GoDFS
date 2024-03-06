package client

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	datanodeService "github.com/Raghav-Tiruvallur/GoDFS/proto/datanode"
	namenodeService "github.com/Raghav-Tiruvallur/GoDFS/proto/namenode"
	"github.com/Raghav-Tiruvallur/GoDFS/utils"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClientData struct {
	NameNodePort string
	Port         string
}

func (client *ClientData) InitializeClient(nameNodePort string) {
	client.NameNodePort = nameNodePort
}
func (client *ClientData) ConnectToNameNode(port string, host string) *grpc.ClientConn {

	connectionString := net.JoinHostPort(host, port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn

}

func GetDataNodeStub(port string) datanodeService.DatanodeServiceClient {
	connectionString := net.JoinHostPort("localhost", port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	dataNodeClient := datanodeService.NewDatanodeServiceClient(conn)
	return dataNodeClient
}

func (client *ClientData) GetAvailableDatanodes(conn *grpc.ClientConn) (*namenodeService.FreeDataNodes, error) {

	namenodeClient := namenodeService.NewNamenodeServiceClient(conn)
	freeDataNodes, err := namenodeClient.GetAvailableDatanodes(context.Background(), &emptypb.Empty{})
	//log.Printf("%s\n", status.StatusMessage)
	return freeDataNodes, err

}

func (client *ClientData) WriteFile(conn *grpc.ClientConn, sourcePath string, fileName string) {

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
		n, err := fileHandler.Read(buffer)
		if err == io.EOF {
			break
		}
		utils.ErrorHandler(err)
		blockID := uuid.New().String()
		freeDataNodes, err := client.GetAvailableDatanodes(conn)
		utils.ErrorHandler(err)
		log.Println(blockID)
		for _, datanode := range freeDataNodes.DataNodeIDs {
			clientDataNodeRequest := &datanodeService.ClientToDataNodeRequest{BlockID: blockID, Content: buffer[:n]}
			datanodeClient := GetDataNodeStub(datanode.DatanodePort)
			log.Printf("Port = %s\n", datanode.DatanodePort)
			status, _ := datanodeClient.SendDataToDataNodes(context.Background(), clientDataNodeRequest)
			log.Println(status.Message)
			log.Printf("%s\n", datanode.DatanodeID)
		}
	}

	//get available datanodes from namenode
	//break the file into many blocks
	//send each block to a datanode
	//datanode then takes care of replicating it across other datanode

}
