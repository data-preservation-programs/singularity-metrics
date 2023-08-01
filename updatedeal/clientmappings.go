package main

import (
	"context"
	"strings"
	"sync"

	"github.com/data-preservation-programs/singularity-metrics/model"
	"github.com/pkg/errors"
	"github.com/ybbus/jsonrpc/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type ClientMappingResolver struct {
	mu                sync.Mutex
	lotusClient       jsonrpc.RPCClient
	mg                *mongo.Client
	actorToAccountKey map[string]model.ClientMapping
	accountKeyToActor map[string]model.ClientMapping
	unresolvable      map[string]struct{}
}

var errNotFound = errors.New("not found")

func (r *ClientMappingResolver) Get(ctx context.Context, id string) (model.ClientMapping, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.unresolvable[id]; ok {
		return model.ClientMapping{}, errNotFound
	}
	if strings.HasPrefix(id, "f0") {
		client, ok := r.actorToAccountKey[id]
		if ok {
			return client, nil
		}
		key, err := r.getAccountKey(ctx, id)
		if err != nil {
			return model.ClientMapping{}, errors.Wrap(err, "failed to get account key")
		}
		client = model.ClientMapping{
			ActorID:    id,
			AccountKey: key,
		}
		result, err := r.mg.Database("singularity").Collection("clients").InsertOne(ctx, client)
		if err != nil {
			return model.ClientMapping{}, errors.Wrap(err, "failed to insert client mapping")
		}
		client.ID = result.InsertedID.(primitive.ObjectID)
		r.accountKeyToActor[key] = client
		r.actorToAccountKey[id] = client
		return client, nil
	}

	client, ok := r.accountKeyToActor[id]
	if ok {
		return client, nil
	}
	actor, err := r.getActorID(ctx, id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			r.unresolvable[id] = struct{}{}
			return model.ClientMapping{}, errNotFound
		}
		return model.ClientMapping{}, errors.Wrap(err, "failed to get actor id")
	}
	client = model.ClientMapping{
		ActorID:    actor,
		AccountKey: id,
	}
	result, err := r.mg.Database("singularity").Collection("clients").InsertOne(ctx, client)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			r.unresolvable[id] = struct{}{}
			return model.ClientMapping{}, errNotFound
		}
		return model.ClientMapping{}, errors.Wrap(err, "failed to insert client mapping")
	}
	client.ID = result.InsertedID.(primitive.ObjectID)
	r.accountKeyToActor[id] = client
	r.actorToAccountKey[actor] = client
	return client, nil
}

func (r *ClientMappingResolver) getActorID(ctx context.Context, long string) (string, error) {
	var out string
	err := r.lotusClient.CallFor(ctx, &out, "Filecoin.StateLookupID", long, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to lookup actor id %s", long)
	}
	return out, nil
}

func (r *ClientMappingResolver) getAccountKey(ctx context.Context, short string) (string, error) {
	var out string
	err := r.lotusClient.CallFor(ctx, &out, "Filecoin.StateAccountKey", short, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to lookup account key %s", short)
	}
	return out, nil
}

func NewClientMappingResolver(ctx context.Context, mg *mongo.Client) (*ClientMappingResolver, error) {
	result, err := mg.Database("singularity").Collection("clients").Find(ctx, bson.M{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client mappings")
	}
	defer result.Close(ctx)
	var clients []model.ClientMapping
	err = result.All(ctx, &clients)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan client mappings")
	}
	var actorToAccountKey = make(map[string]model.ClientMapping)
	var accountKeyToActor = make(map[string]model.ClientMapping)
	for _, v := range clients {
		actorToAccountKey[v.ActorID] = v
		accountKeyToActor[v.AccountKey] = v
	}
	return &ClientMappingResolver{
		lotusClient:       jsonrpc.NewClientWithOpts("https://api.node.glif.io/", &jsonrpc.RPCClientOpts{}),
		mg:                mg,
		actorToAccountKey: actorToAccountKey,
		accountKeyToActor: accountKeyToActor,
		unresolvable:      make(map[string]struct{}),
	}, nil
}
