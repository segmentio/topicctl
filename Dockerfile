FROM golang:1.14 as builder
ENV SRC github.com/segmentio/topicctl
ENV CGO_ENABLED=0

ARG VERSION
RUN test -n "${VERSION}"

COPY . /go/src/${SRC}
RUN cd /go/src/${SRC} && make install VERSION=${VERSION}

FROM alpine:3

RUN apk update && apk add bash curl
COPY --from=builder /go/bin/topicctl /usr/local/bin/topicctl
