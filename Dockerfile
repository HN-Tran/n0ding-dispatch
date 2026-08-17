FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/n0ding-dispatch ./cmd/n0ding-dispatch \
    && mkdir -p /out/data \
    && chown 65532:65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build --chown=65532:65532 /out/data /data
WORKDIR /data
COPY --from=build /out/n0ding-dispatch /usr/local/bin/n0ding-dispatch
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/n0ding-dispatch"]
CMD ["serve","--addr","0.0.0.0:8080","--db","/data/dispatch.db"]
