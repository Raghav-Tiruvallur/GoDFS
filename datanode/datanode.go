package datanode

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	datanodeService "github.com/Raghav-Tiruvallur/GoDFS/proto/datanode"
	namenodeService "github.com/Raghav-Tiruvallur/GoDFS/proto/namenode"
	"github.com/Raghav-Tiruvallur/GoDFS/utils"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DataNode struct {
	ID               string
	DataNodeLocation string
	Blocks           []string
	datanodeService.UnimplementedDatanodeServiceServer
}

func (datanode *DataNode) ConnectToNameNode(port string, host string) *grpc.ClientConn {

	connectionString := net.JoinHostPort(host, port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn

}

func CreateDirectory(path string) {

	_, err := os.Stat(path)

	if os.IsNotExist(err) {

		pathCreationError := os.MkdirAll(path, os.ModePerm)
		utils.ErrorHandler(pathCreationError)
	} else {
		utils.ErrorHandler(err)
	}
}

func (datanode *DataNode) SendDataToDataNodes(ctx context.Context, clientToDataNodeRequest *datanodeService.ClientToDataNodeRequest) (*datanodeService.Status, error) {

	CreateDirectory(datanode.DataNodeLocation)
	blockFilePath := filepath.Join(datanode.DataNodeLocation, clientToDataNodeRequest.BlockID+".txt")
	err := os.WriteFile(blockFilePath, clientToDataNodeRequest.Content, os.ModePerm)
	datanode.Blocks = append(datanode.Blocks, clientToDataNodeRequest.BlockID)
	utils.ErrorHandler(err)
	return &datanodeService.Status{Message: "Data saved successfully"}, nil
}

func (datanode *DataNode) RegisterNode(conn *grpc.ClientConn, port string) {
	client := namenodeService.NewNamenodeServiceClient(conn)
	status, err := client.Register_DataNode(context.Background(), &namenodeService.DatanodeData{DatanodeID: datanode.ID, DatanodePort: port})
	log.Printf("%s\n", status.StatusMessage)
	utils.ErrorHandler(err)
}

func (datanode *DataNode) SendBlockReport(conn *grpc.ClientConn) {

	nameNodeClient := namenodeService.NewNamenodeServiceClient(conn)
	datanodeBlockData := &namenodeService.DatanodeBlockData{DatanodeID: datanode.ID, Blocks: datanode.Blocks}
	status, err := nameNodeClient.BlockReport(context.Background(), datanodeBlockData)
	utils.ErrorHandler(err)
	log.Println(status.StatusMessage)
}
func (datanode *DataNode) InitializeDataNode(port string, location string) {
	datanode.ID = uuid.New().String()
	datanode.DataNodeLocation = filepath.Join(location, datanode.ID)
}

func (datanode *DataNode) SendBlockReportToNameNode(conn *grpc.ClientConn) {

	interval := 10 * time.Second

	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			datanode.SendBlockReport(conn)
		}
	}

}
func (datanode *DataNode) StartServer(port string) {
	server := grpc.NewServer()
	datanodeService.RegisterDatanodeServiceServer(server, datanode)
	address := ":" + port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Datanode with id = %s is listening on port %s\n", datanode.ID, address)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
