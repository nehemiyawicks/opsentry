package decode

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
)

type storeABIAdapter struct {
	load func(ctx context.Context, chainID uint64, address [20]byte) ([]byte, bool, error)
	save func(ctx context.Context, chainID uint64, address [20]byte, abiJSON []byte) error
}

func (a *storeABIAdapter) LoadCachedABI(ctx context.Context, chainID uint64, address common.Address) ([]byte, bool, error) {
	var addr [20]byte
	copy(addr[:], address.Bytes())
	return a.load(ctx, chainID, addr)
}

func (a *storeABIAdapter) SaveCachedABI(ctx context.Context, chainID uint64, address common.Address, abiJSON []byte) error {
	var addr [20]byte
	copy(addr[:], address.Bytes())
	return a.save(ctx, chainID, addr, abiJSON)
}

func NewStoreABICache(
	load func(ctx context.Context, chainID uint64, address [20]byte) ([]byte, bool, error),
	save func(ctx context.Context, chainID uint64, address [20]byte, abiJSON []byte) error,
) ABICache {
	return &storeABIAdapter{load: load, save: save}
}
