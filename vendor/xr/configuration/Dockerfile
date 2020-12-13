FROM golang:latest

RUN go get github.com/tools/godep

COPY *.go /go/src/xr/configuration/
COPY Godeps /go/src/xr/configuration/Godeps

WORKDIR /go/src/xr/configuration/
CMD sleep 5 && godep go test -v --cover