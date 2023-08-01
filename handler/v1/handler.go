package v1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/data-preservation-programs/singularity-metrics/model/v1model"
	"github.com/klauspost/compress/zstd"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var decoder *zstd.Decoder
var client *mongo.Client

func init() {
	var err error
	decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic(err)
	}
	client, err = mongo.Connect(context.Background(), options.Client().ApplyURI(os.Getenv("MONGODB_URI")))
	if err != nil {
		panic(err)
	}
}

func handleError(err error, msg string, status int) (events.APIGatewayProxyResponse, error) {
	err = errors.Wrap(err, msg)
	log.Println(err.Error())
	return events.APIGatewayProxyResponse{Body: err.Error(), StatusCode: status}, nil
}

func HandleRequest(ctx context.Context, request events.APIGatewayV2HTTPRequest) (events.APIGatewayProxyResponse, error) {
	// Decode the request body with base64
	decoded, err := base64.StdEncoding.DecodeString(request.Body)
	if err != nil {
		return handleError(err, "failed to decode the body", 400)
	}
	// Decompress using zstd
	decoded, err = decoder.DecodeAll(decoded, nil)
	if err != nil {
		return handleError(err, "failed to decompress the body", 400)
	}
	var v1Events []v1model.Event
	err = json.Unmarshal(decoded, &v1Events)
	if err != nil {
		return handleError(err, "failed to unmarshal the body", 400)
	}

	log.Printf("Received %d events\n", len(v1Events))
	var cars []any
	var deals []any
	for _, event := range v1Events {
		switch event.Type {
		case "deal_proposed":
			var dealProposal v1model.DealProposalEvent
			err = mapstructure.Decode(event.Values, &dealProposal)
			if err != nil {
				return handleError(err, "failed to decode deal_proposed event", 400)
			}
			deal := dealProposal.ToDeal(event.Timestamp, event.Instance, request.RequestContext.HTTP.SourceIP)
			deals = append(deals, deal)

		case "generation_complete":
			var generation v1model.GenerationCompleteEvent
			err = mapstructure.Decode(event.Values, &generation)
			if err != nil {
				return handleError(err, "failed to decode generation_complete event", 400)
			}
			car := generation.ToCar(event.Timestamp, event.Instance, request.RequestContext.HTTP.SourceIP)
			cars = append(cars, car)
		}
	}

	log.Printf("Inserting %d cars and %d deals\n", len(cars), len(deals))
	if len(cars) > 0 {
		_, err = client.Database("singularity").Collection("cars").InsertMany(ctx, cars)
		if err != nil {
			return handleError(err, "failed to insert piece records", 500)
		}
	}
	if len(deals) > 0 {
		_, err = client.Database("singularity").Collection("deals").InsertMany(ctx, deals)
		if err != nil {
			return handleError(err, "failed to insert deal records", 500)
		}
	}
	return events.APIGatewayProxyResponse{Body: fmt.Sprintf("Inserted %d cars and %d deals", len(cars), len(deals)), StatusCode: 200}, nil
}
