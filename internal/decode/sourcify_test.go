package decode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestSourcifyV2Success(t *testing.T) {
	body := `{"abi":[{"type":"event","name":"Ping","inputs":[]}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/server/v2/contract/") {
			t.Errorf("expected v2 path, got %s", r.URL.Path)
		}
		if r.URL.Query().Get("fields") != "abi" {
			t.Errorf("expected ?fields=abi, got %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	f := &SourcifyFetcher{BaseURL: srv.URL, HTTPClient: srv.Client()}
	a, err := f.Fetch(context.Background(), 8453, common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Events["Ping"]; !ok {
		t.Fatal("expected Ping event present")
	}
}

func TestSourcify404IsExplicit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	f := &SourcifyFetcher{BaseURL: srv.URL, HTTPClient: srv.Client()}
	_, err := f.Fetch(context.Background(), 1, common.HexToAddress("0x0"))
	if err == nil || !strings.Contains(err.Error(), "no verified contract") {
		t.Fatalf("expected explicit not-verified error, got %v", err)
	}
}

func TestSourcifyServerErrorSurfacesStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := &SourcifyFetcher{BaseURL: srv.URL, HTTPClient: srv.Client()}
	_, err := f.Fetch(context.Background(), 1, common.HexToAddress("0x0"))
	if err == nil || !strings.Contains(err.Error(), "http 500") {
		t.Fatalf("expected http 500 in error, got %v", err)
	}
}

func TestSourcifyEmptyABIFieldIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	f := &SourcifyFetcher{BaseURL: srv.URL, HTTPClient: srv.Client()}
	_, err := f.Fetch(context.Background(), 1, common.HexToAddress("0x0"))
	if err == nil || !strings.Contains(err.Error(), "empty abi") {
		t.Fatalf("expected empty-abi error, got %v", err)
	}
}
