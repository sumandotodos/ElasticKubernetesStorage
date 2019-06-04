# build stage
FROM golang:latest AS builder
# working directory
WORKDIR /go/src/github.com/agiratech/docker_imgs
COPY controller.go .
# rebuilt built in libraries and disabled cgo
RUN go get -d -v
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o controller .
# final stage
FROM alpine:latest
# working directory
WORKDIR /go/src/github.com/agiratech/docker_imgs
# copy the binary file into working directory
COPY --from=builder /go/src/github.com/agiratech/docker_imgs/controller .
# Run the docker_imgs command when the container starts.
CMD ["./controller"]
EXPOSE 2222
