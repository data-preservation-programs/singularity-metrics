package v1model

import (
	"time"

	"github.com/data-preservation-programs/singularity-metrics/model"
)

type Event struct {
	Timestamp int64          `json:"timestamp"`
	Instance  string         `json:"instance"`
	Type      string         `json:"type"`
	Values    map[string]any `json:"values"`
}

type GenerationCompleteEvent struct {
	DatasetID                   string  `mapstructure:"datasetId" json:"datasetId"`
	DatasetName                 string  `mapstructure:"datasetName" json:"datasetName"`
	GenerationID                string  `mapstructure:"generationId" json:"generationId"`
	Index                       int64   `mapstructure:"index" json:"index"`
	PieceSize                   int64   `mapstructure:"pieceSize" json:"pieceSize"`
	PieceCID                    string  `mapstructure:"pieceCid" json:"pieceCid"`
	CarSize                     int64   `mapstructure:"carSize" json:"carSize"`
	NumOfFiles                  int64   `mapstructure:"numOfFiles" json:"numOfFiles"`
	TimeSpentInGenerationMs     float64 `mapstructure:"timeSpentInGenerationMs" json:"timeSpentInGenerationMs"`
	TimeSpentInMovingToTmpdirMs float64 `mapstructure:"timeSpendInMovingToTmpdirMs" json:"timeSpendInMovingToTmpdirMs"`
}

func (e GenerationCompleteEvent) ToCar(timestamp int64, instanceID string, ip string) model.Car {
	return model.Car{
		Reporter: model.Reporter{
			IsV1:       true,
			InstanceID: instanceID,
			IP:         ip,
		},
		DatasetName: e.DatasetName,
		CreatedAt:   time.Unix(timestamp, 0),
		CarID:       e.Index,
		PieceCID:    e.PieceCID,
		PieceSize:   e.PieceSize,
		FileSize:    e.CarSize,
		NumOfFiles:  e.NumOfFiles,
		TimeSpent:   time.Duration((e.TimeSpentInGenerationMs + e.TimeSpentInMovingToTmpdirMs) * float64(time.Millisecond)),
	}
}

type DealProposalEvent struct {
	Protocol    string  `mapstructure:"protocol" json:"protocol"`
	PieceCID    string  `mapstructure:"pieceCid" json:"pieceCid"`
	DataCID     string  `mapstructure:"dataCid" json:"dataCid"`
	PieceSize   int64   `mapstructure:"pieceSize" json:"pieceSize"`
	CarSize     int64   `mapstructure:"carSize" json:"carSize"`
	Provider    string  `mapstructure:"provider" json:"provider"`
	Client      string  `mapstructure:"client" json:"client"`
	Verified    bool    `mapstructure:"verified" json:"verified"`
	Duration    int32   `mapstructure:"duration" json:"duration"`
	ProposalCID string  `mapstructure:"proposalCid" json:"proposalCid"`
	Price       float64 `mapstructure:"price" json:"price"`
}

func (e DealProposalEvent) ToDeal(timestamp int64, instanceID string, ip string) model.Deal {
	return model.Deal{
		Reporter: model.Reporter{
			IsV1:       true,
			InstanceID: instanceID,
			IP:         ip,
		},
		CreatedAt: time.Unix(timestamp, 0),
		Client:    e.Client,
		Provider:  e.Provider,
		Label:     e.DataCID,
		PieceCID:  e.PieceCID,
		PieceSize: e.PieceSize,
		Verified:  e.Verified,
		Price:     e.Price,
		Duration:  e.Duration,
		State:     "proposed",
	}
}
