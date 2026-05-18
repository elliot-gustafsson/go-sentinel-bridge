FROM docker.io/golang:1.26 AS builder

WORKDIR /usr/src/app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build

FROM gcr.io/distroless/static-debian13

USER nonroot:nonroot

COPY --from=builder --chown=nonroot:nonroot /usr/src/app/go-sentinel-bridge /usr/local/bin/

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/go-sentinel-bridge"]
