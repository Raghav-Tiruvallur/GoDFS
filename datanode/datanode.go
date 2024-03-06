package datanode

import (
	"context"
	"log"
	"net"
	"path/filepath"

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
	datanodeService.UnimplementedDatanodeServiceServer
}

func (datanode *DataNode) ConnectToNameNode(port string, host string) *grpc.ClientConn {

	connectionString := net.JoinHostPort(host, port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn

}

func (datanode *DataNode) RegisterNode(conn *grpc.ClientConn, port string) {
	client := namenodeService.NewNamenodeServiceClient(conn)
	status, err := client.Register_DataNode(context.Background(), &namenodeService.DatanodeData{DatanodeID: datanode.ID, DatanodePort: port})
	log.Printf("%s\n", status.StatusMessage)
	utils.ErrorHandler(err)
}

func (datanode *DataNode) InitializeDataNode(port string, location string) {
	datanode.ID = uuid.New().String()
	datanode.DataNodeLocation = filepath.Join(location, datanode.ID)
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
