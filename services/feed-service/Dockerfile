FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/feed-service ./cmd/server

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/feed-service /app/feed-service
CMD ["/app/feed-service"]
