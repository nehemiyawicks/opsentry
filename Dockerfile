FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /opsentry ./cmd/opsentry

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /opsentry /opsentry
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/opsentry"]
