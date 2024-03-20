package client

import (
	"context"
	"io"
	"log"
	"math/rand"
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

type Block struct {
	blockID string
}

func (client *ClientData) InitializeClient(nameNodePort string) {
	client.NameNodePort = nameNodePort
}
func (client *ClientData) ConnectToNameNode() *grpc.ClientConn {

	connectionString := net.JoinHostPort("localhost", client.NameNodePort)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn

}

func GetDataNodeStub(port string) datanodeService.DatanodeServiceClient {
	connectionString := net.JoinHostPort("localhost", port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	dataNodeClient := datanodeService.NewDatanodeServiceClient(conn)
	return dataNodeClient
}

func (client *ClientData) GetNameNodeStub() namenodeService.NamenodeServiceClient {
	connectionString := net.JoinHostPort("localhost", client.NameNodePort)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	nameNodeClient := namenodeService.NewNamenodeServiceClient(conn)
	return nameNodeClient
}

func (client *ClientData) GetAvailableDatanodes(conn *grpc.ClientConn) (*namenodeService.FreeDataNodes, error) {

	namenodeClient := namenodeService.NewNamenodeServiceClient(conn)
	freeDataNodes, err := namenodeClient.GetAvailableDatanodes(context.Background(), &emptypb.Empty{})
	return freeDataNodes, err

}

func ThreadDone(done chan string, blockID string) {
	done <- blockID
}

func SendData(dataNodeID string, datanodePort string, done chan string, blockID string, buffer []byte, n int) {
	clientDataNodeRequest := &datanodeService.ClientToDataNodeRequest{BlockID: blockID, Content: buffer[:n]}
	defer ThreadDone(done, blockID)
	datanodeClient := GetDataNodeStub(datanodePort)
	log.Printf("Port = %s\n", datanodePort)
	status, _ := datanodeClient.SendDataToDataNodes(context.Background(), clientDataNodeRequest)
	log.Println(status.Message)
	log.Printf("%s\n", dataNodeID)
}

func (client *ClientData) ProcessData(conn *grpc.ClientConn, blockSize int, done chan string, filePath string) {

	blockID := uuid.New().String()
	buffer := make([]byte, blockSize)

	fileHandler, err := os.Open(filePath)

	utils.ErrorHandler(err)
	n, err := fileHandler.Read(buffer)
	if err == io.EOF {
		return
	}
	utils.ErrorHandler(err)
	freeDataNodes, err := client.GetAvailableDatanodes(conn)
	utils.ErrorHandler(err)
	for _, datanode := range freeDataNodes.DataNodeIDs {
		go SendData(datanode.DatanodeID, datanode.DatanodePort, done, blockID, buffer, n)
	}
}

func (client *ClientData) SendFileBlockMappingToNameNode(filePath string, blockIDs []string) {

	nameNodeStub := client.GetNameNodeStub()
	fileBlockMetadata := &namenodeService.FileBlockMetadata{FilePath: filePath, BlockIDs: blockIDs}
	status, err := nameNodeStub.FileBlockMapping(context.Background(), fileBlockMetadata)
	utils.ErrorHandler(err)
	log.Println("Sent file block mapping to namenode with status:", status.StatusMessage)
}

func (client *ClientData) WriteFile(conn *grpc.ClientConn, sourcePath string, fileName string) {

	filePath := filepath.Join(sourcePath, fileName)

	blockSize := int(3 * 1024)
	fileSizeHandler, err := os.Stat(filePath)

	utils.ErrorHandler(err)

	fileSize := int(fileSizeHandler.Size())

	numberOfBlocks := fileSize / blockSize

	if fileSize%blockSize > 0 {
		numberOfBlocks++
	}
	done := make(chan string)
	defer close(done)
	for i := 0; i < numberOfBlocks; i++ {
		go client.ProcessData(conn, blockSize, done, filePath)
	}
	blockIDs := make([]string, 0)
	for i := 0; i < numberOfBlocks; i++ {
		blockIDs = append(blockIDs, <-done)
	}
	client.SendFileBlockMappingToNameNode(filePath, blockIDs)

}

func (client *ClientData) ReadFile(conn *grpc.ClientConn, source string, fileName string) {

	filePath := filepath.Join(source, fileName)
	nameNodeStub := client.GetNameNodeStub()
	fileData := &namenodeService.FileData{FileName: filePath}
	dataNodes, err := nameNodeStub.GetDataNodesForFile(context.Background(), fileData)
	utils.ErrorHandler(err)
	rand.Seed(10)
	dataNodesBlocks := dataNodes.BlockDataNodes
	for _, blockDataNode := range dataNodesBlocks {
		blockID := blockDataNode.BlockID
		log.Println(blockID)
		dataNodeIDs := blockDataNode.DataNodeIDs
		log.Println(len(dataNodeIDs))
		dataNodeIdx := rand.Intn(len(dataNodeIDs))
		dataNode := dataNodeIDs[dataNodeIdx]
		log.Println(dataNode.DatanodePort)
		dataNodeClient := GetDataNodeStub(dataNode.DatanodePort)
		blockRequest := &datanodeService.BlockRequest{BlockID: blockID}
		blockResponse, err := dataNodeClient.ReadBytesFromDataNode(context.Background(), blockRequest)
		utils.ErrorHandler(err)
		log.Println(string(blockResponse.FileContent))
	}

}
