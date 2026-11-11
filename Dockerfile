FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/agentsquare ./cmd/agentsquare

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/agentsquare /usr/local/bin/agentsquare
COPY --from=builder /src/spec /opt/agentsquare/spec
COPY --from=builder /src/web/viewer /opt/agentsquare/web/viewer
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/agentsquare"]
CMD ["serve", "--addr", ":8080", "--ledger", "/data/ledger.jsonl"]
