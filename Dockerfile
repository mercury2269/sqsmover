FROM golang:1.16-alpine as builder
WORKDIR /app
COPY go.mod go.sum main.go /app/
RUN go get .
RUN go build -o sqsmover .

FROM alpine:3.13.5
COPY --from=builder /app/sqsmover /usr/local/bin/sqsmover
CMD sqsmover -s ${SQS_DEAD_QUEUE_NAME} -d ${SQS_QUEUE_NAME}
