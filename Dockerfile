# build topicctl
FROM --platform=$BUILDPLATFORM golang:1.22 as builder
ENV SRC github.com/getsentry/topicctl
ENV CGO_ENABLED=0

ARG VERSION
RUN test -n "${VERSION}"

COPY . /go/src/${SRC}

ARG TARGETOS TARGETARCH
RUN cd /go/src/${SRC} && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH make topicctl VERSION=${VERSION}

# copy topicctl & scripts to python image
FROM python:3.12-alpine

COPY --from=builder \
    /go/src/github.com/getsentry/topicctl/build/topicctl \
    /bin/topicctl
COPY --from=builder \
    /go/src/github.com/getsentry/topicctl/scripts \
    /bin/scripts

ENTRYPOINT ["python", "/bin/scripts/call_topicctl.py"]
