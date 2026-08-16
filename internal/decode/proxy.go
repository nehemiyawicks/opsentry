package decode

import (
	"context"
	"math/big"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type StorageReader interface {
	StorageAt(ctx context.Context, address common.Address, slot common.Hash) ([]byte, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
}

var proxyImplSlots = []common.Hash{
	// EIP-1967: bytes32(uint256(keccak256("eip1967.proxy.implementation")) - 1)
	common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"),
	// OpenZeppelin unstructured storage (older; used by Circle's FiatTokenProxy and many tokens):
	// bytes32(keccak256("org.zeppelinos.proxy.implementation"))
	common.HexToHash("0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"),
}

// EIP-1967 beacon slot: bytes32(uint256(keccak256("eip1967.proxy.beacon")) - 1)
var eip1967BeaconSlot = common.HexToHash("0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50")

func looksLikeProxy(a abi.ABI) bool {
	for _, name := range []string{"Upgraded", "AdminChanged", "BeaconUpgraded"} {
		if _, ok := a.Events[name]; ok {
			return true
		}
	}
	return false
}

func readImplementationAddress(ctx context.Context, reader StorageReader, proxy common.Address) (common.Address, bool, error) {
	for _, slot := range proxyImplSlots {
		data, err := reader.StorageAt(ctx, proxy, slot)
		if err != nil {
			continue
		}
		if len(data) < 20 || allZero(data) {
			continue
		}
		return common.BytesToAddress(data), true, nil
	}
	beaconData, err := reader.StorageAt(ctx, proxy, eip1967BeaconSlot)
	if err != nil {
		return common.Address{}, false, err
	}
	if len(beaconData) < 20 || allZero(beaconData) {
		return common.Address{}, false, nil
	}
	beacon := common.BytesToAddress(beaconData)
	implSelector := crypto.Keccak256([]byte("implementation()"))[:4]
	result, err := reader.CallContract(ctx, ethereum.CallMsg{To: &beacon, Data: implSelector}, nil)
	if err != nil {
		return common.Address{}, false, err
	}
	if len(result) < 32 || allZero(result) {
		return common.Address{}, false, nil
	}
	return common.BytesToAddress(result[12:32]), true, nil
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
