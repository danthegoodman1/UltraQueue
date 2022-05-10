FROM golang:1.18.1 as build

WORKDIR /app

COPY go.* /app/
RUN go mod download

COPY . .

RUN go build -o /app/thebin

# Need glibc
FROM gcr.io/distroless/base
COPY --from=build /app/thebin /app/

ENTRYPOINT ["/app/thebin"]
