package v1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/data-preservation-programs/singularity-metrics/model"
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

type Event struct {
	Timestamp int64          `json:"timestamp"`
	Instance  string         `json:"instance"`
	Type      string         `json:"type"`
	Values    map[string]any `json:"values"`
}

type GenerationCompleteEvent struct {
	DatasetID                   string `mapstructure:"datasetId"`
	DatasetName                 string `mapstructure:"datasetName"`
	GenerationID                string `mapstructure:"generationId"`
	Index                       int64  `mapstructure:"index"`
	PieceSize                   int64  `mapstructure:"pieceSize"`
	PieceCID                    string `mapstructure:"pieceCid"`
	CarSize                     int64  `mapstructure:"carSize"`
	NumOfFiles                  int64  `mapstructure:"numOfFiles"`
	TimeSpentInGenerationMs     int64  `mapstructure:"timeSpentInGenerationMs"`
	TimeSpentInMovingToTmpdirMs int64  `mapstructure:"timeSpendInMovingToTmpdirMs"`
}

type DealProposalEvent struct {
	Protocol    string  `mapstructure:"protocol"`
	PieceCID    string  `mapstructure:"pieceCid"`
	DataCID     string  `mapstructure:"dataCid"`
	PieceSize   int64   `mapstructure:"pieceSize"`
	CarSize     int64   `mapstructure:"carSize"`
	Provider    string  `mapstructure:"provider"`
	Client      string  `mapstructure:"client"`
	Verified    bool    `mapstructure:"verified"`
	Duration    int32   `mapstructure:"duration"`
	ProposalCID string  `mapstructure:"proposalCid"`
	Price       float64 `mapstructure:"price"`
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
	var v1Events []Event
	err = json.Unmarshal(decoded, &v1Events)
	if err != nil {
		return handleError(err, "failed to unmarshal the body", 400)
	}

	log.Printf("Received %d events\n", len(v1Events))
	var cars []any
	var deals []any
	for _, event := range v1Events {
		reporter := model.Reporter{
			IsV1:       true,
			InstanceID: event.Instance,
			IP:         request.RequestContext.HTTP.SourceIP,
		}
		switch event.Type {
		case "deal_proposed":
			var dealProposal DealProposalEvent
			err = mapstructure.Decode(event.Values, &dealProposal)
			if err != nil {
				return handleError(err, "failed to decode deal_proposed event", 400)
			}
			deal := model.Deal{
				Reporter:  reporter,
				Client:    dealProposal.Client,
				Provider:  dealProposal.Provider,
				Label:     dealProposal.DataCID,
				PieceCID:  dealProposal.PieceCID,
				PieceSize: dealProposal.PieceSize,
				Verified:  dealProposal.Verified,
				Price:     dealProposal.Price,
				Duration:  dealProposal.Duration,
				DealOnChain: model.DealOnChain{
					State: "proposed",
				},
			}
			deals = append(deals, deal)

		case "generation_complete":
			var generation GenerationCompleteEvent
			err = mapstructure.Decode(event.Values, &generation)
			if err != nil {
				return handleError(err, "failed to decode generation_complete event", 400)
			}
			car := model.Car{
				Reporter:    reporter,
				DatasetName: generation.DatasetName,
				CreatedAt:   time.Unix(event.Timestamp, 0),
				CarID:       generation.Index,
				PieceCID:    generation.PieceCID,
				PieceSize:   generation.PieceSize,
				FileSize:    generation.CarSize,
				NumOfFiles:  generation.NumOfFiles,
				TimeSpent:   time.Duration(generation.TimeSpentInGenerationMs+generation.TimeSpentInMovingToTmpdirMs) * time.Millisecond,
			}
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
