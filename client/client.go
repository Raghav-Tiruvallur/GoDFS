package client

import (
	goDFS "github.com/Raghav-Tiruvallur/GoDFS/proto"
)

type ClientData struct {
	goDFS.UnimplementedGODFSServer
	NameNodePort string
	Port         string
}

func (client *ClientData) InitializeClient(nameNodePort string) {
	client.NameNodePort = nameNodePort
}
