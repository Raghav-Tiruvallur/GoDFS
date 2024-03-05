package client

type ClientData struct {
	NameNodePort string
	Port         string
}

func (client *ClientData) InitializeClient(nameNodePort string) {
	client.NameNodePort = nameNodePort
}
