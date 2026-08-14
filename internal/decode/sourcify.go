package decode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

type SourcifyFetcher struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewSourcifyFetcher() *SourcifyFetcher {
	return &SourcifyFetcher{
		BaseURL:    "https://sourcify.dev",
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (f *SourcifyFetcher) Fetch(ctx context.Context, chainID uint64, address common.Address) (abi.ABI, error) {
	url := fmt.Sprintf("%s/server/v2/contract/%d/%s?fields=abi", f.BaseURL, chainID, address.Hex())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return abi.ABI{}, err
	}
	resp, err := f.HTTPClient.Do(req)
	if err != nil {
		return abi.ABI{}, fmt.Errorf("sourcify %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return abi.ABI{}, fmt.Errorf("sourcify: no verified contract at %d/%s", chainID, address.Hex())
	}
	if resp.StatusCode != http.StatusOK {
		return abi.ABI{}, fmt.Errorf("sourcify %s: http %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
	if err != nil {
		return abi.ABI{}, err
	}
	var payload struct {
		ABI json.RawMessage `json:"abi"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return abi.ABI{}, fmt.Errorf("parse response: %w", err)
	}
	if len(payload.ABI) == 0 {
		return abi.ABI{}, fmt.Errorf("sourcify response has empty abi field")
	}
	return abi.JSON(bytes.NewReader(payload.ABI))
}
