FROM golang:1.23-alpine AS builder

ENV PATH="/go/bin:${PATH}"
ENV GO111MODULE=on
ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /go/src

RUN apk -U add ca-certificates
RUN apk update && apk upgrade && apk add pkgconf git bash build-base sudo
RUN apk update && apk add librdkafka-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -tags musl --ldflags "-extldflags -static" -o producer ./producer

FROM scratch AS runner

COPY --from=builder /go/src/producer /

ENTRYPOINT ["./producer"]