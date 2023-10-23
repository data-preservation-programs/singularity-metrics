package v2

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/data-preservation-programs/singularity-metrics/model"
	"github.com/data-preservation-programs/singularity/analytics"
	"github.com/fxamacker/cbor/v2"
	"github.com/gotidy/ptr"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"github.com/rjNemo/underscore"
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

func ToCar(event analytics.PackJobEvent, ip string) model.Car {
	var outputType *string
	if event.OutputType != "" {
		outputType = ptr.Of(event.OutputType)
	} else {
		outputType = ptr.Of("inline")
	}
	return model.Car{
		Reporter: model.Reporter{
			IsV1:       false,
			InstanceID: event.Instance,
			IP:         ip,
			Identity:   event.Identity,
		},
		SourceType: ptr.Of(event.SourceType),
		OutputType: outputType,
		CreatedAt:  time.Unix(event.Timestamp, 0),
		PieceCID:   event.PieceCID,
		PieceSize:  event.PieceSize,
		FileSize:   event.CarSize,
		NumOfFiles: event.NumOfFiles,
	}
}

func ToDeal(event analytics.DealProposalEvent, ip string) model.Deal {
	return model.Deal{
		Reporter: model.Reporter{
			IsV1:       false,
			InstanceID: event.Instance,
			IP:         ip,
			Identity:   event.Identity,
		},
		CreatedAt:  time.Unix(event.Timestamp, 0),
		Client:     event.Client,
		Provider:   event.Provider,
		Label:      event.DataCID,
		PieceCID:   event.PieceCID,
		PieceSize:  event.PieceSize,
		Verified:   event.Verified,
		Duration:   event.EndEpoch - event.StartEpoch,
		State:      "proposed",
		StartEpoch: ptr.Of(event.StartEpoch),
		EndEpoch:   ptr.Of(event.EndEpoch),
	}
}

func HandleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
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

	var v2events analytics.Events
	err = cbor.NewDecoder(bytes.NewReader(decoded)).Decode(&v2events)
	if err != nil {
		return handleError(err, "failed to unmarshal the body", 400)
	}

	log.Printf("Received %d pack v2events and %d deal v2events\n", len(v2events.PackJobEvents), len(v2events.DealEvents))

	cars := underscore.Map(v2events.PackJobEvents, func(event analytics.PackJobEvent) any {
		return ToCar(event, request.RequestContext.Identity.SourceIP)
	})
	deals := underscore.Map(v2events.DealEvents, func(event analytics.DealProposalEvent) any {
		return ToDeal(event, request.RequestContext.Identity.SourceIP)
	})

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
