# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder
ENV GOPROXY=https://proxy.golang.org|https://goproxy.cn|direct
ENV GOSUMDB=off
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILDTIME=unknown
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w \
        -X github.com/kxj/gos3/internal/version.Version=${VERSION} \
        -X github.com/kxj/gos3/internal/version.Commit=${COMMIT} \
        -X github.com/kxj/gos3/internal/version.BuildTime=${BUILDTIME}" \
      -o /out/gos3 .

FROM alpine:3.20 AS runtime
RUN adduser -D -H -u 10001 gos3 \
    && mkdir -p /data \
    && chown gos3:gos3 /data
COPY --from=builder /out/gos3 /usr/local/bin/gos3
USER gos3
EXPOSE 9000
VOLUME ["/data"]
HEALTHCHECK --interval=2s --timeout=3s --start-period=3s --retries=30 \
  CMD wget -qO- http://127.0.0.1:9000/healthz || exit 1
ENTRYPOINT ["gos3"]
CMD ["server", "/data"]

FROM alpine:3.20 AS verify
RUN apk add --no-cache bash curl coreutils
COPY --from=minio/mc:latest /usr/bin/mc /usr/local/bin/mc
COPY scripts/verify.sh /usr/local/bin/verify.sh
RUN chmod +x /usr/local/bin/verify.sh
ENTRYPOINT ["/usr/local/bin/verify.sh"]
