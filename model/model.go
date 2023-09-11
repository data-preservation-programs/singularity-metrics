package model

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Reporter struct {
	IsV1       bool   `bson:"isV1"`
	InstanceID string `bson:"instanceId"`
	IP         string `bson:"ip"`
}

type Car struct {
	Reporter    `bson:",inline"`
	DatasetID   *uint32       `bson:"datasetId,omitempty"`
	DatasetName string        `bson:"datasetName,omitempty"`
	SourceType  *string       `bson:"sourceType,omitempty"`
	OutputType  *string       `bson:"outputType,omitempty"`
	SourceID    *uint32       `bson:"sourceId,omitempty"`
	CreatedAt   time.Time     `bson:"createdAt"`
	CarID       int64         `bson:"carId,omitempty"`
	PieceCID    string        `bson:"pieceCid"`
	PieceSize   int64         `bson:"pieceSize"`
	FileSize    int64         `bson:"fileSize"`
	NumOfFiles  int64         `bson:"numOfFiles"`
	TimeSpent   time.Duration `bson:"timeSpent,omitempty"`
}

type Deal struct {
	ID               primitive.ObjectID `bson:"_id,omitempty"`
	Reporter         `bson:",inline"`
	CreatedAt        time.Time `bson:"createdAt"`
	DealID           *uint64   `bson:"dealId,omitempty"`
	DatasetID        *uint32   `bson:"datasetId,omitempty"`
	Client           string    `bson:"client"`
	Provider         string    `bson:"provider"`
	Label            string    `bson:"label"`
	PieceCID         string    `bson:"pieceCid"`
	PieceSize        int64     `bson:"pieceSize"`
	State            string    `bson:"state"`
	StartEpoch       *int32    `bson:"startEpoch,omitempty"`
	SectorStartEpoch *int32    `bson:"sectorStartEpoch,omitempty"`
	Duration         int32     `bson:"duration,omitempty"`
	EndEpoch         *int32    `bson:"endEpoch,omitempty"`
	Verified         bool      `bson:"verified"`
	KeepUnsealed     *bool     `bson:"keepUnsealed,omitempty"`
	Price            float64   `bson:"price"` // Fil per epoch per GiB
}

type ClientMapping struct {
	ID         primitive.ObjectID `bson:"_id,omitempty"`
	ActorID    string             `bson:"actorId"`
	AccountKey string             `bson:"accountKey"`
}
