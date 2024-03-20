BINARY_NAME="go-dfs"

for ((i = 1; i <= 10; i++)); do
  osascript -e "tell application \"Terminal\" to do script \"cd $(pwd) && ./${BINARY_NAME} datanode -port $((8000 + $i)) -location datanode-files\""
done
