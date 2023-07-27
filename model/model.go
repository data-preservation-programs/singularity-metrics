package model

import "time"

type Reporter struct {
	IsV1       bool   `bson:"isV1"`
	InstanceID string `bson:"instanceId"`
	IP         string `bson:"ip"`
}

type Car struct {
	Reporter    `bson:",inline"`
	DatasetID   *uint32       `bson:"datasetId,omitempty"`
	DatasetName string        `bson:"datasetName"`
	SourceType  *string       `bson:"sourceType,omitempty"`
	SourceID    *uint32       `bson:"sourceId,omitempty"`
	CreatedAt   time.Time     `bson:"createdAt"`
	CarID       int64         `bson:"carId"`
	PieceCID    string        `bson:"pieceCid"`
	PieceSize   int64         `bson:"pieceSize"`
	FileSize    int64         `bson:"fileSize"`
	NumOfFiles  int64         `bson:"numOfFiles"`
	TimeSpent   time.Duration `bson:"timeSpent"`
}

type DealOnChain struct {
	DealID           *uint64 `bson:"dealId,omitempty"`
	State            string  `bson:"state"`
	SectorStartEpoch *int32  `bson:"sectorStartEpoch,omitempty"`
}

type Deal struct {
	Reporter     `bson:",inline"`
	DealOnChain  `bson:",inline"`
	DealID       *uint64 `bson:"dealId,omitempty"`
	DatasetID    *uint32 `bson:"datasetId,omitempty"`
	Client       string  `bson:"client"`
	Provider     string  `bson:"provider"`
	Label        string  `bson:"label"`
	PieceCID     string  `bson:"pieceCid"`
	PieceSize    int64   `bson:"pieceSize"`
	StartEpoch   *int32  `bson:"startEpoch,omitempty"`
	Duration     int32   `bson:"duration,omitempty"`
	Verified     bool    `bson:"verified"`
	KeepUnsealed *bool   `bson:"keepUnsealed,omitempty"`
	IPNI         *bool   `bson:"ipni,omitempty"`
	ScheduleCron *string `bson:"scheduleCron,omitempty"`
	Price        float64 `bson:"price"` // Fil per epoch per GiB
}
