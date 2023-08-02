package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bcicen/jstream"
	"github.com/data-preservation-programs/singularity-metrics/model"
	_ "github.com/joho/godotenv/autoload"
	"github.com/klauspost/compress/zstd"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const dealsCollection = "deals"

func getAllPieceCIDs(ctx context.Context, mg *mongo.Client) (map[string]struct{}, map[string]struct{}, error) {
	result, err := mg.Database("singularity").Collection("cars").Aggregate(ctx, bson.A{
		bson.M{
			"$group": bson.M{
				"_id": bson.M{
					"isV1":     "$isV1",
					"pieceCid": "$pieceCid",
				},
			},
		},
		bson.M{
			"$project": bson.M{
				"isV1":     "$_id.isV1",
				"pieceCid": "$_id.pieceCid",
			},
		},
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to query cars")
	}
	defer result.Close(ctx)
	var cars []struct {
		IsV1     bool   `bson:"isV1"`
		PieceCID string `bson:"pieceCid"`
	}
	if err := result.All(ctx, &cars); err != nil {
		return nil, nil, errors.Wrap(err, "failed to decode cars")
	}
	v1 := make(map[string]struct{})
	v2 := make(map[string]struct{})
	for _, car := range cars {
		if car.IsV1 {
			v1[car.PieceCID] = struct{}{}
		} else {
			v2[car.PieceCID] = struct{}{}
		}
	}
	log.Printf("found %d v1 pieceCIDs and %d v2 pieceCIDs\n", len(v1), len(v2))
	return v1, v2, nil
}

func epochToTimestamp(epoch int32) time.Time {
	return time.Unix(int64(epoch)*30+1598306400, 0)
}

func timestampToEpoch(t time.Time) int32 {
	return int32(t.Unix()-1598306400) / 30
}

func yesterdayEpoch() int32 {
	return timestampToEpoch(time.Now().Add(-time.Hour * 24))
}

func saveDealAsExternal(ctx context.Context, mg *mongo.Client, dealID uint64, deal MarketDeal, isV1 bool) error {
	price, err := strconv.ParseFloat(deal.Proposal.StoragePricePerEpoch, 64)
	if err != nil {
		return errors.Wrap(err, "failed to parse storage price per epoch")
	}
	// convert from attoFIL per epoch to FIL per GiB per epoch
	price = price / 1e18 / float64(deal.Proposal.PieceSize) * (1 << 30)
	var state = deal.getState()
	d := model.Deal{
		Reporter: model.Reporter{
			IsV1:       isV1,
			InstanceID: "external",
		},
		CreatedAt:        epochToTimestamp(deal.Proposal.StartEpoch),
		DealID:           &dealID,
		Client:           deal.Proposal.Client,
		Provider:         deal.Proposal.Provider,
		Label:            deal.Proposal.Label,
		PieceCID:         deal.Proposal.PieceCID.Root,
		PieceSize:        int64(deal.Proposal.PieceSize),
		State:            state,
		StartEpoch:       &deal.Proposal.StartEpoch,
		SectorStartEpoch: &deal.State.SectorStartEpoch,
		Duration:         deal.Proposal.EndEpoch - deal.Proposal.StartEpoch,
		EndEpoch:         &deal.Proposal.EndEpoch,
		Verified:         deal.Proposal.VerifiedDeal,
		Price:            price,
	}
	if _, err := mg.Database("singularity").Collection(dealsCollection).InsertOne(ctx, d); err != nil {
		return errors.Wrap(err, "failed to insert deal")
	}
	log.Printf("saved deal %d as external\n", dealID)
	return nil
}

func updateDeal(ctx context.Context, mg *mongo.Client, id primitive.ObjectID, state string, dealID uint64, marketDeal MarketDeal) error {
	var newState = marketDeal.getState()
	if state == newState {
		return nil
	}
	result, err := mg.Database("singularity").Collection(dealsCollection).UpdateOne(ctx, bson.M{
		"_id": id,
	}, bson.M{
		"$set": bson.M{
			"state":            newState,
			"dealId":           dealID,
			"startEpoch":       marketDeal.Proposal.StartEpoch,
			"sectorStartEpoch": marketDeal.State.SectorStartEpoch,
			"endEpoch":         marketDeal.Proposal.EndEpoch,
			"duration":         marketDeal.Proposal.EndEpoch - marketDeal.Proposal.StartEpoch,
		},
	})
	if err != nil {
		return errors.Wrap(err, "failed to update deal")
	}
	if result.MatchedCount == 0 {
		return errors.Errorf("deal not found %s", id)
	} else {
		log.Printf("update state for deal %d: %s\n", dealID, newState)
	}
	return nil
}

type UnknownDeal struct {
	ID       primitive.ObjectID `bson:"_id"`
	Client   string             `bson:"client"`
	Provider string             `bson:"provider"`
	PieceCID string             `bson:"pieceCid"`
	Label    string             `bson:"label"`
}

func getAllUnknownDeals(ctx context.Context, mg *mongo.Client, clientResolver *ClientMappingResolver) (map[string][]UnknownDeal, error) {
	result, err := mg.Database("singularity").Collection(dealsCollection).Find(ctx,
		bson.M{"dealId": bson.M{"$exists": false}},
		&options.FindOptions{
			Projection: bson.M{
				"_id":      1,
				"client":   1,
				"provider": 1,
				"pieceCid": 1,
				"label":    1,
			},
			Sort: bson.M{"createdAt": 1},
		})
	if err != nil {
		return nil, errors.Wrap(err, "failed to find unknown deals")
	}
	defer result.Close(ctx)
	var deals []UnknownDeal
	err = result.All(ctx, &deals)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan unknown deals")
	}
	log.Printf("found %d unknown deals\n", len(deals))

	unknownDealsMap := make(map[string][]UnknownDeal)
	for _, v := range deals {
		client, err := clientResolver.Get(ctx, v.Client)
		if errors.Is(err, errNotFound) {
			key := fmt.Sprintf("%s|%s|%s", v.Client, v.Provider, v.PieceCID)
			unknownDealsMap[key] = append(unknownDealsMap[key], v)
			continue
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to resolve client")
		}
		key := fmt.Sprintf("%s|%s|%s", client.ActorID, v.Provider, v.PieceCID)
		unknownDealsMap[key] = append(unknownDealsMap[key], v)
	}
	return unknownDealsMap, nil
}

type KnownDeal struct {
	ID    primitive.ObjectID `bson:"_id"`
	State string             `bson:"state"`
}

func getKnownDeals(ctx context.Context, mg *mongo.Client) (map[uint64]KnownDeal, error) {
	var r []struct {
		ID     primitive.ObjectID `bson:"_id"`
		DealID uint64             `bson:"dealId"`
		State  string             `bson:"state"`
	}
	result, err := mg.Database("singularity").Collection(dealsCollection).Find(
		ctx,
		bson.M{"dealId": bson.M{"$exists": true}},
		&options.FindOptions{
			Projection: bson.M{"dealId": 1, "state": 1},
		})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get known deal ids")
	}
	defer result.Close(ctx)
	err = result.All(ctx, &r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan known deal ids")
	}
	var ids = make(map[uint64]KnownDeal)
	for _, v := range r {
		ids[v.DealID] = KnownDeal{
			ID:    v.ID,
			State: v.State,
		}
	}
	log.Printf("found %d known deals\n", len(ids))
	return ids, nil
}

type VerifiedClient struct {
	ID        int32  `json:"id" bson:"id"`
	AddressID string `json:"addressId" bson:"addressId"`
	Address   string `json:"address" bson:"address"`
	Name      string `json:"name" bson:"name"`
	OrgName   string `json:"orgName" bson:"orgName"`
	Region    string `json:"region" bson:"region"`
	Website   string `json:"website" bson:"website"`
	Industry  string `json:"industry" bson:"industry"`
}

func updateVerifiedClients(ctx context.Context, mg *mongo.Client) error {
	resp, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.datacapstats.io/api/getVerifiedClients", nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}
	client := http.DefaultClient
	result, err := client.Do(resp)
	if err != nil {
		return errors.Wrap(err, "failed to get verified clients")
	}
	defer result.Body.Close()
	var respBody struct {
		Data []VerifiedClient `json:"data"`
	}
	err = json.NewDecoder(result.Body).Decode(&respBody)
	if err != nil {
		return errors.Wrap(err, "failed to decode response")
	}
	for _, vClient := range respBody.Data {
		updateResult, err := mg.Database("singularity").Collection("verifiedClients").UpdateOne(ctx,
			bson.M{"id": vClient.ID}, bson.M{"$set": vClient}, options.Update().SetUpsert(true))
		if err != nil {
			return errors.Wrap(err, "failed to update verified client")
		}
		if updateResult.UpsertedCount > 0 {
			log.Printf("inserted verified client %d\n", vClient.ID)
		} else {
			log.Printf("updated verified client %d\n", vClient.ID)
		}
	}
	return nil
}

func run(ctx context.Context) error {
	mg, err := mongo.Connect(context.Background(), options.Client().ApplyURI(os.Getenv("MONGODB_URI")))
	if err != nil {
		return errors.Wrap(err, "failed to connect to mongo")
	}

	err = updateVerifiedClients(ctx, mg)
	if err != nil {
		return errors.Wrap(err, "failed to update verified clients")
	}

	clientResolver, err := NewClientMappingResolver(ctx, mg)
	if err != nil {
		return errors.Wrap(err, "failed to create client mapping resolver")
	}

	unknownDealsMap, err := getAllUnknownDeals(ctx, mg, clientResolver)
	if err != nil {
		return errors.Wrap(err, "failed to get unknown deals")
	}

	knownDeals, err := getKnownDeals(ctx, mg)
	if err != nil {
		return errors.Wrap(err, "failed to get known deal ids")
	}
	v1CIDs, v2CIDs, err := getAllPieceCIDs(ctx, mg)
	if err != nil {
		return errors.Wrap(err, "failed to get all piece cids")
	}

	req, err := http.NewRequestWithContext(ctx,
		http.MethodGet,
		"https://marketdeals.s3.amazonaws.com/StateMarketDeals.json.zst",
		nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to make request")
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("failed to get state market deals: %s", resp.Status)
	}

	decompressor, err := zstd.NewReader(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to create decompressor")
	}

	defer decompressor.Close()

	jsonDecoder := jstream.NewDecoder(decompressor, 1).EmitKV()
	for stream := range jsonDecoder.Stream() {
		keyValuePair, ok := stream.Value.(jstream.KV)

		if !ok {
			return errors.New("failed to get key value pair")
		}

		var deal MarketDeal
		err = mapstructure.Decode(keyValuePair.Value, &deal)
		if err != nil {
			return errors.Wrap(err, "failed to decode deal")
		}
		dealIdNum, err := strconv.ParseUint(keyValuePair.Key, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse deal id")
		}

		// Save the result to database anyway
		_, err = clientResolver.Get(ctx, deal.Proposal.Client)

		// If the deal is already in the list, check if it needs to be updated
		if knownDeal, ok := knownDeals[dealIdNum]; ok {
			err = updateDeal(ctx, mg, knownDeal.ID, knownDeal.State, dealIdNum, deal)
			if err != nil {
				return errors.Wrap(err, "failed to update deal")
			}
			continue
		}

		key := fmt.Sprintf("%s|%s|%s", deal.Proposal.Client, deal.Proposal.Provider, deal.Proposal.PieceCID.Root)
		if unknownDeals, ok := unknownDealsMap[key]; ok {
			err = updateDeal(ctx, mg, unknownDeals[0].ID, "proposed", dealIdNum, deal)
			if err != nil {
				return errors.Wrap(err, "failed to mark deal active")
			}
			if len(unknownDeals) == 1 {
				delete(unknownDealsMap, key)
			} else {
				unknownDealsMap[key] = unknownDeals[1:]
			}
			continue
		}

		if _, ok := v2CIDs[deal.Proposal.PieceCID.Root]; ok {
			err = saveDealAsExternal(ctx, mg, dealIdNum, deal, false)
			if err != nil {
				return errors.Wrap(err, "failed to save deal as external")
			}
			continue
		}

		if _, ok := v1CIDs[deal.Proposal.PieceCID.Root]; ok {
			err = saveDealAsExternal(ctx, mg, dealIdNum, deal, true)
			if err != nil {
				return errors.Wrap(err, "failed to save deal as external")
			}
			continue
		}
	}

	currentEpoch := yesterdayEpoch()
	markExpiredResult, err := mg.Database("singularity").Collection(dealsCollection).UpdateMany(
		ctx, bson.M{"state": "active", "endEpoch": bson.M{"$lt": currentEpoch}}, bson.M{"$set": bson.M{"state": "expired"}})
	if err != nil {
		return errors.Wrap(err, "failed to mark expired deals")
	}
	log.Printf("marked %d deals as expired\n", markExpiredResult.ModifiedCount)
	markProposalExpiredResult, err := mg.Database("singularity").Collection(dealsCollection).UpdateMany(
		ctx, bson.M{"state": bson.M{"$in": bson.A{"proposed", "published"}}, "$or": bson.A{
			bson.M{"startEpoch": bson.M{"$lt": currentEpoch}},
			bson.M{"createdAt": bson.M{"$lt": time.Now().Add(-time.Hour * 24 * 30)}},
		}},
		bson.M{"$set": bson.M{"state": "proposal_expired"}})
	if err != nil {
		return errors.Wrap(err, "failed to mark expired proposal deals")
	}
	log.Printf("marked %d proposal deals as expired\n", markProposalExpiredResult.ModifiedCount)
	return nil
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		panic(err)
	}
}
