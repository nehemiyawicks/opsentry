package ingest

import (
	"context"
	"fmt"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type ChainReader interface {
	BlockByNumber(ctx context.Context, n uint64) (pipeline.BlockRef, error)
}

type ReconcileResult struct {
	Canonical []pipeline.BlockRef
	Reverted  []pipeline.BlockRef
}

type Reconciler struct {
	chain    string
	maxDepth uint64
	reader   ChainReader

	known  map[uint64]pipeline.BlockRef
	tip    uint64
	seeded bool
}

func NewReconciler(chain string, maxDepth uint64, reader ChainReader) *Reconciler {
	if maxDepth == 0 {
		maxDepth = 256
	}
	return &Reconciler{
		chain:    chain,
		maxDepth: maxDepth,
		reader:   reader,
		known:    make(map[uint64]pipeline.BlockRef),
	}
}

func (r *Reconciler) Tip() (pipeline.BlockRef, bool) {
	if !r.seeded {
		return pipeline.BlockRef{}, false
	}
	return r.known[r.tip], true
}

func (r *Reconciler) LoadKnown(blocks []pipeline.BlockRef) {
	for _, b := range blocks {
		r.known[b.Number] = b
		if b.Number > r.tip {
			r.tip = b.Number
		}
		r.seeded = true
	}
	r.prune()
}

func (r *Reconciler) OnHead(ctx context.Context, head pipeline.BlockRef) (ReconcileResult, error) {
	if !r.seeded {
		r.record(head)
		return ReconcileResult{Canonical: []pipeline.BlockRef{head}}, nil
	}
	if b, ok := r.known[head.Number]; ok && b.Hash == head.Hash {
		return ReconcileResult{}, nil
	}
	if head.Number > r.tip {
		return r.applyForward(ctx, head)
	}
	return r.handleReorg(ctx, head)
}

func (r *Reconciler) applyForward(ctx context.Context, head pipeline.BlockRef) (ReconcileResult, error) {
	originalTip := r.tip
	fetched := make([]pipeline.BlockRef, 0, head.Number-originalTip)
	for n := originalTip + 1; n <= head.Number; n++ {
		var b pipeline.BlockRef
		if n == head.Number {
			b = head
		} else {
			got, err := r.reader.BlockByNumber(ctx, n)
			if err != nil {
				return ReconcileResult{}, fmt.Errorf("fetch %d during catchup: %w", n, err)
			}
			b = got
		}
		var prev pipeline.BlockRef
		if n == originalTip+1 {
			prev = r.known[originalTip]
		} else {
			prev = fetched[len(fetched)-1]
		}
		if b.ParentHash != prev.Hash {
			return r.handleReorg(ctx, head)
		}
		fetched = append(fetched, b)
	}
	for _, b := range fetched {
		r.record(b)
	}
	return ReconcileResult{Canonical: fetched}, nil
}

func (r *Reconciler) handleReorg(ctx context.Context, head pipeline.BlockRef) (ReconcileResult, error) {
	cursor := head
	ancestryPath := []pipeline.BlockRef{head}
	var commonAncestor uint64
	ancestorFound := false

	for cursor.Number > 0 {
		parentNum := cursor.Number - 1
		if parentInOld, ok := r.known[parentNum]; ok {
			if parentInOld.Hash == cursor.ParentHash {
				commonAncestor = parentNum
				ancestorFound = true
				break
			}
		} else if parentNum < r.tip && !r.hasAnyBelow(parentNum+1) {
			commonAncestor = parentNum
			ancestorFound = true
			break
		}
		got, err := r.reader.BlockByNumber(ctx, parentNum)
		if err != nil {
			return ReconcileResult{}, fmt.Errorf("walk back to %d: %w", parentNum, err)
		}
		if got.Hash != cursor.ParentHash {
			return ReconcileResult{}, fmt.Errorf("chain shifted at %d during reconciliation", parentNum)
		}
		cursor = got
		ancestryPath = append(ancestryPath, cursor)
	}
	if !ancestorFound {
		commonAncestor = 0
	}

	for i, j := 0, len(ancestryPath)-1; i < j; i, j = i+1, j-1 {
		ancestryPath[i], ancestryPath[j] = ancestryPath[j], ancestryPath[i]
	}

	result := ReconcileResult{}
	oldTip := r.tip
	for n := commonAncestor + 1; n <= oldTip; n++ {
		if b, ok := r.known[n]; ok {
			result.Reverted = append(result.Reverted, b)
			delete(r.known, n)
		}
	}
	r.tip = commonAncestor

	for _, b := range ancestryPath {
		r.record(b)
		result.Canonical = append(result.Canonical, b)
	}
	return result, nil
}

func (r *Reconciler) record(b pipeline.BlockRef) {
	r.known[b.Number] = b
	if b.Number > r.tip {
		r.tip = b.Number
	}
	r.seeded = true
	r.prune()
}

func (r *Reconciler) prune() {
	if r.tip <= r.maxDepth {
		return
	}
	threshold := r.tip - r.maxDepth
	for n := range r.known {
		if n < threshold {
			delete(r.known, n)
		}
	}
}

func (r *Reconciler) hasAnyBelow(n uint64) bool {
	for k := range r.known {
		if k < n {
			return true
		}
	}
	return false
}
