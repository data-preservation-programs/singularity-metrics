package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/data-preservation-programs/singularity-metrics/model/v1model"
	"github.com/jackc/pgx/v5"
	_ "github.com/joho/godotenv/autoload"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Event struct {
	Type      string `db:"type"`
	Timestamp int64  `db:"timestamp"`
	IP        string `db:"ip"`
	Instance  string `db:"instance"`
	Values    string `db:"values"`
}

func main() {
	ctx := context.Background()
	db, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	mg, err := mongo.Connect(context.Background(), options.Client().ApplyURI(os.Getenv("MONGODB_URI")))
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	query, err := db.Query(ctx, "SELECT type, timestamp, ip, instance, values FROM events WHERE type = 'deal_proposed'")
	if err != nil {
		panic(err)
	}

	var events []Event

	for query.Next() {
		var event Event
		err = query.Scan(&event.Type, &event.Timestamp, &event.IP, &event.Instance, &event.Values)
		if err != nil {
			panic(err)
		}
		events = append(events, event)
		if len(events) == 10000 {
			save(ctx, mg, events)
			if err != nil {
				panic(err)
			}
			events = []Event{}
		}
	}

	save(ctx, mg, events)
}

func save(ctx context.Context, client *mongo.Client, events []Event) {
	var cars []any
	var deals []any
	for _, event := range events {
		switch event.Type {
		case "generation_complete":
			var e v1model.GenerationCompleteEvent
			err := json.Unmarshal([]byte(event.Values), &e)
			if err != nil {
				panic(err)
			}
			cars = append(cars, e.ToCar(event.Timestamp, event.Instance, event.IP))
		case "deal_proposed":
			var e v1model.DealProposalEvent
			err := json.Unmarshal([]byte(event.Values), &e)
			if err != nil {
				panic(err)
			}
			deals = append(deals, e.ToDeal(event.Timestamp, event.Instance, event.IP))
		}
	}
	log.Printf("Inserting %d cars and %d deals\n", len(cars), len(deals))
	if len(cars) > 0 {
		_, err := client.Database("singularity").Collection("cars").InsertMany(ctx, cars)
		if err != nil {
			panic(err)
		}
	}
	if len(deals) > 0 {
		_, err := client.Database("singularity").Collection("deals").InsertMany(ctx, deals)
		if err != nil {
			panic(err)
		}
	}
}
