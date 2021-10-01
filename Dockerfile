FROM golang:1.17 as builder
ENV SRC github.com/segmentio/topicctl
ENV CGO_ENABLED=0

ARG VERSION
RUN test -n "${VERSION}"

COPY . /go/src/${SRC}
RUN cd /go/src/${SRC} && make install VERSION=${VERSION}

FROM scratch

COPY --from=builder /go/bin/topicctl /bin/topicctl
ENTRYPOINT ["/bin/topicctl"]
