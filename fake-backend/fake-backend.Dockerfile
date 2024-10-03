FROM golang:1.23-alpine AS builder

ENV PATH="/go/bin:${PATH}"
ENV GO111MODULE=off
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /go/src

COPY main.go main.go

RUN go build -o fake-backend

FROM scratch AS runner

COPY --from=builder /go/src/fake-backend /

EXPOSE 8999

ENTRYPOINT ["./fake-backend"]
