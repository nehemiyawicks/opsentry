package decode

import (
	"context"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

type StorageReader interface {
	StorageAt(ctx context.Context, address common.Address, slot common.Hash) ([]byte, error)
}

var proxyImplSlots = []common.Hash{
	// EIP-1967: bytes32(uint256(keccak256("eip1967.proxy.implementation")) - 1)
	common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"),
	// OpenZeppelin unstructured storage (older; used by Circle's FiatTokenProxy and many tokens):
	// bytes32(keccak256("org.zeppelinos.proxy.implementation"))
	common.HexToHash("0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"),
}

func looksLikeProxy(a abi.ABI) bool {
	for _, name := range []string{"Upgraded", "AdminChanged", "BeaconUpgraded"} {
		if _, ok := a.Events[name]; ok {
			return true
		}
	}
	return false
}

func readImplementationAddress(ctx context.Context, reader StorageReader, proxy common.Address) (common.Address, bool, error) {
	var lastErr error
	for _, slot := range proxyImplSlots {
		data, err := reader.StorageAt(ctx, proxy, slot)
		if err != nil {
			lastErr = err
			continue
		}
		if len(data) < 20 || allZero(data) {
			continue
		}
		return common.BytesToAddress(data), true, nil
	}
	return common.Address{}, false, lastErr
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
