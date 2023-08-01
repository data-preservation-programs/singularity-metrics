package main

type MarketDeal struct {
	Proposal DealProposal
	State    DealState
}

func (deal MarketDeal) getState() string {
	var state string
	if deal.State.SlashEpoch > 0 {
		state = "slashed"
	} else if deal.State.SectorStartEpoch > 0 {
		state = "active"
	} else if deal.Proposal.EndEpoch < yesterdayEpoch() {
		state = "expired"
	} else if deal.Proposal.StartEpoch < yesterdayEpoch() {
		state = "proposal_expired"
	} else {
		state = "published"
	}
	return state
}

type Cid struct {
	Root string `json:"/" mapstructure:"/"`
}

type DealProposal struct {
	PieceCID             Cid
	PieceSize            uint64
	VerifiedDeal         bool
	Client               string
	Provider             string
	Label                string
	StartEpoch           int32
	EndEpoch             int32
	StoragePricePerEpoch string
}

type DealState struct {
	SectorStartEpoch int32
	LastUpdatedEpoch int32
	SlashEpoch       int32
}
