FROM golang:1.13-stretch AS builder
MAINTAINER Hector Sanjuan <hector@protocol.ai>

# This dockerfile builds and runs dbserver.

ENV SRC_PATH   /go/github.com/ipfs/go-ds-crdt
ENV GO111MODULE on

# COPY go.mod go.sum $SRC_PATH/
# WORKDIR $SRC_PATH
# RUN go mod download

# COPY . $SRC_PATH/
# RUN go build 
# RUN cp dbserver /usr/local/bin/dbserver

# Uncomment to copy manually built binary
COPY dbserver /usr/local/bin/dbserver

ENV PORT 6666
ENTRYPOINT ["/usr/local/bin/dbserver"]
CMD []
