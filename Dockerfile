# Imagem única com os binários Go (tracking-service, analytics-service, seed e connect-watchdog).
# O modo de propagação é escolhido em tempo de execução por PROPAGATION_MODE,
# de modo que os dois protótipos rodam exatamente o mesmo código compilado.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
# O wget do busybox (já presente) atende aos healthchecks.
COPY --from=build /out/ /app/
ENV TZ=UTC
EXPOSE 8080 8081 8086
CMD ["/app/tracking-service"]
