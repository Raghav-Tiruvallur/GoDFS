FROM alpine

WORKDIR /app

COPY --from=godfs-base /app/scripts . 
COPY --from=godfs-base /app/go-dfs . 

CMD [ "sh" , "/app/scripts/run_datanodes.sh" ]