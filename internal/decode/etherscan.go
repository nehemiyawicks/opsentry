package decode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type EtherscanFetcher struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

func NewEtherscanFetcher(apiKey string) *EtherscanFetcher {
	return &EtherscanFetcher{
		BaseURL:    "https://api.etherscan.io/v2/api",
		APIKey:     apiKey,
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (f *EtherscanFetcher) FetchJSON(ctx context.Context, chainID uint64, address common.Address) ([]byte, error) {
	if f.APIKey == "" {
		return nil, fmt.Errorf("etherscan: no api key configured")
	}
	q := url.Values{}
	q.Set("chainid", fmt.Sprintf("%d", chainID))
	q.Set("module", "contract")
	q.Set("action", "getabi")
	q.Set("address", address.Hex())
	q.Set("apikey", f.APIKey)
	full := f.BaseURL + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("etherscan: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("etherscan: http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
	if err != nil {
		return nil, err
	}
	var payload struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Result  string `json:"result"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("etherscan parse: %w", err)
	}
	if payload.Status != "1" {
		return nil, fmt.Errorf("etherscan: %s (%s)", payload.Message, truncate(payload.Result, 120))
	}
	if payload.Result == "" {
		return nil, fmt.Errorf("etherscan: empty result")
	}
	return []byte(payload.Result), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
