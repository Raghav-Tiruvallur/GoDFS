#!/bin/bash

BINARY_NAME="go-dfs"

for ((i = 1; i <= 10; i++)); do
  gnome-terminal -- bash -c "cd $(pwd) && ./${BINARY_NAME} datanode -port $((8000 + $i)) -location datanode-files; exec bash"
done
