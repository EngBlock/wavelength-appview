FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o appview .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /app/appview /usr/local/bin/appview
EXPOSE 4000
CMD ["appview"]
