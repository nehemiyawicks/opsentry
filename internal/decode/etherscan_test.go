package decode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestEtherscanFetchSuccess(t *testing.T) {
	abi := `[{"type":"event","name":"Ping","inputs":[]}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("apikey") != "test-key" {
			t.Errorf("expected apikey=test-key, got %s", r.URL.Query().Get("apikey"))
		}
		if r.URL.Query().Get("chainid") != "8453" {
			t.Errorf("expected chainid=8453, got %s", r.URL.Query().Get("chainid"))
		}
		if r.URL.Query().Get("action") != "getabi" {
			t.Errorf("expected action=getabi, got %s", r.URL.Query().Get("action"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"1","message":"OK","result":` + toJSONString(abi) + `}`))
	}))
	defer srv.Close()

	f := &EtherscanFetcher{BaseURL: srv.URL, APIKey: "test-key", HTTPClient: srv.Client()}
	data, err := f.FetchJSON(context.Background(), 8453, common.HexToAddress("0x1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != abi {
		t.Fatalf("expected ABI passthrough, got %s", data)
	}
}

func TestEtherscanRateLimitedIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"0","message":"NOTOK","result":"Max rate limit reached"}`))
	}))
	defer srv.Close()

	f := &EtherscanFetcher{BaseURL: srv.URL, APIKey: "k", HTTPClient: srv.Client()}
	_, err := f.FetchJSON(context.Background(), 1, common.HexToAddress("0x1"))
	if err == nil || !strings.Contains(err.Error(), "rate limit") {
		t.Fatalf("expected rate-limit error, got %v", err)
	}
}

func TestEtherscanNoKeyFails(t *testing.T) {
	f := &EtherscanFetcher{APIKey: ""}
	_, err := f.FetchJSON(context.Background(), 1, common.HexToAddress("0x1"))
	if err == nil || !strings.Contains(err.Error(), "no api key") {
		t.Fatalf("expected no-api-key error, got %v", err)
	}
}

func toJSONString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
