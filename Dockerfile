FROM --platform=$BUILDPLATFORM golang:1.21 as builder
ENV SRC github.com/segmentio/topicctl
ENV CGO_ENABLED=0

ARG VERSION
RUN test -n "${VERSION}"

COPY . /go/src/${SRC}

ARG TARGETOS TARGETARCH
RUN cd /go/src/${SRC} && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH make topicctl VERSION=${VERSION}

FROM scratch

COPY --from=builder \
    /go/src/github.com/segmentio/topicctl/build/topicctl \
    /bin/topicctl
ENTRYPOINT ["/bin/topicctl"]
