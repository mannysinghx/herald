FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /herald .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 herald && mkdir /data && chown herald /data
COPY --from=build /herald /usr/local/bin/herald
USER herald
ENV HERALD_DB=/data/herald.db
EXPOSE 8080
ENTRYPOINT ["herald"]
CMD ["serve"]
