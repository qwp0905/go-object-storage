FROM golang:1.20 AS build
ARG TARGETOS
ARG TARGETARCH

ARG ENTRY_DIR

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/
COPY pkg/ pkg/

RUN CGO_ENABLED=0 \
  GOOS=${TARGETOS:-linux} \
  GOARCH=${TARGETARCH} \
  go build -a -o execute cmd/${ENTRY_DIR}/main.go

FROM debian:bookworm-20230919-slim

WORKDIR /
COPY --from=build /workspace/execute .

VOLUME [ "/data" ]

ENTRYPOINT ["/execute"]