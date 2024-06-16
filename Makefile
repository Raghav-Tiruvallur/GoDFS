GOCMD = "go"
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test

BINARY_NAME=go-dfs

build:
	$(GOBUILD) -o $(BINARY_NAME) -v main.go

run:
	$(GOBUILD) -o $(BINARY_NAME) -v main.go

run-namenode:
	make run
	./$(BINARY_NAME) namenode -port 8080 -block-size 32
run-client-write:
	make run
	./$(BINARY_NAME) client -namenode 8080 -operation write -source-path . -filename big.txt
run-client-read:
	make run
	./$(BINARY_NAME) client -namenode 8080 -operation read -source-path . -filename big.txt
run-datanodes:
	make run
	sh scripts/run_datanodes.sh
deps:
	$(GOGET) -v ./..

protoc: 
	sh scripts/generate_proto.sh