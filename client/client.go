package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"

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

type Pair[T any, V any] struct {
	first  T
	second V
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

func ThreadDone(done chan Pair[int, string], blockID string, idx int) {

	done <- Pair[int, string]{first: idx, second: blockID}
}
func SendData(dataNodeID string, datanodePort string, done chan Pair[int, string], blockID string, buffer []byte, n int, idx int) {
	clientDataNodeRequest := &datanodeService.ClientToDataNodeRequest{BlockID: blockID, Content: buffer[:n]}
	datanodeClient := GetDataNodeStub(datanodePort)
	_, _ = datanodeClient.SendDataToDataNodes(context.Background(), clientDataNodeRequest)
	ThreadDone(done, blockID, idx)

}

func (client *ClientData) ProcessData(conn *grpc.ClientConn, blockSize int, done chan Pair[int, string], filePath string, start int, idx int) {

	blockID := uuid.New().String()

	fileHandler, err := os.Open(filePath)

	utils.ErrorHandler(err)
	fileInfo, err := fileHandler.Stat()
	fileSize := fileInfo.Size()
	end := start + blockSize
	if end > int(fileSize) {
		end = int(fileSize)
	}
	readBytes := end - start
	buffer := make([]byte, readBytes)
	n, err := fileHandler.ReadAt(buffer, int64(start))
	if err == io.EOF {
		return
	}
	utils.ErrorHandler(err)
	freeDataNodes, err := client.GetAvailableDatanodes(conn)
	utils.ErrorHandler(err)
	var wg sync.WaitGroup
	for _, datanode := range freeDataNodes.DataNodeIDs {
		wg.Add(1)
		go func(datanode *namenodeService.DatanodeData) {
			defer wg.Done()
			SendData(datanode.DatanodeID, datanode.DatanodePort, done, blockID, buffer, n, idx)
		}(datanode)
	}
	wg.Wait()
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

	var wg sync.WaitGroup

	done := make(chan Pair[int, string])
	startList := make([]int64, 0)

	amount := 0

	for {
		if amount > fileSize {
			break
		}
		startList = append(startList, int64(amount))
		amount += blockSize

	}
	for i := 0; i < numberOfBlocks; i++ {
		wg.Add(1)
		go func(start int, idx int) {
			defer wg.Done()
			client.ProcessData(conn, blockSize, done, filePath, start, idx)
		}(int(startList[i]), i)
	}
	wg.Wait()
	sortedblockIDs := make([]Pair[int, string], 0)
	for i := 0; i < numberOfBlocks; i++ {
		sortedblockIDs = append(sortedblockIDs, <-done)
	}
	for _, block := range sortedblockIDs {
		fmt.Printf("Before = %d\n", block.first)
	}
	sort.Slice(sortedblockIDs, func(i, j int) bool {
		return sortedblockIDs[i].first < sortedblockIDs[j].first
	})
	blockIDs := make([]string, 0)
	for _, block := range sortedblockIDs {
		fmt.Printf("After = %d\n", block.first)
		blockIDs = append(blockIDs, block.second)
	}
	client.SendFileBlockMappingToNameNode(filePath, blockIDs)
	close(done)

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
		dataNodeIDs := blockDataNode.DataNodeIDs
		dataNodeIdx := rand.Intn(len(dataNodeIDs))
		dataNode := dataNodeIDs[dataNodeIdx]
		dataNodeClient := GetDataNodeStub(dataNode.DatanodePort)
		blockRequest := &datanodeService.BlockRequest{BlockID: blockID}
		blockResponse, err := dataNodeClient.ReadBytesFromDataNode(context.Background(), blockRequest)
		utils.ErrorHandler(err)
		log.Println(string(blockResponse.FileContent))
	}

}
