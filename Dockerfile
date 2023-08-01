FROM public.ecr.aws/docker/library/golang:1.19-alpine as builder
WORKDIR /app
COPY . .
RUN go build -o update ./updatedeal

FROM public.ecr.aws/docker/library/alpine:latest
WORKDIR /app
COPY --from=builder /app/update .
CMD ["/app/update"]
