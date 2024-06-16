FROM golang:1.22.4-alpine3.20 AS godfs-base

WORKDIR /app

COPY go.mod go.sum /app/

RUN apk add --no-cache make

RUN go mod download

COPY . .

RUN make build
