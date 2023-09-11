package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	v2 "github.com/data-preservation-programs/singularity-metrics/handler/v2"
)

func main() {
	lambda.Start(v2.HandleRequest)
}
