FROM alpine

WORKDIR /app

COPY --from=godfs-base /app/go-dfs . 

CMD [ "./go-dfs" , "namenode", "-port" , "8080" , "-block-size", "32" ]