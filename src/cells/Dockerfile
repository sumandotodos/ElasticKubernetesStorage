# build stage
FROM golang:latest AS builder
# working directory
WORKDIR /go/src/github.com/agiratech/docker_imgs
COPY storagecell.go .
# rebuilt built in libraries and disabled cgo
RUN go get -d -v
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o storagecell .
# final stage
FROM alpine:latest
# working directory
WORKDIR /go/src/github.com/agiratech/docker_imgs
# copy the binary file into working directory
COPY --from=builder /go/src/github.com/agiratech/docker_imgs/storagecell .
# Run the docker_imgs command when the container starts.
CMD ["./storagecell"]
EXPOSE 7777
