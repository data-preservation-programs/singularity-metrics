package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/data-preservation-programs/singularity-metrics/handler/v1"
)

func main() {
	lambda.Start(v1.HandleRequest)
}
