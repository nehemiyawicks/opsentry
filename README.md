# opsentry

[![CI](https://github.com/nehemiyawicks/opsentry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nehemiyawicks/opsentry/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/nehemiyawicks/opsentry)](https://goreportcard.com/report/github.com/nehemiyawicks/opsentry)

Open-source contract monitoring and alerting for the OP Stack. Self-hostable, Apache-2.0.

## What it is

A single self-hostable service that watches contracts on Base, OP Mainnet, and any OP-Stack rollup. Rules are YAML: event matches, function-call filters, thresholds, and `eth_call` invariants. Alerts fan out to Slack, Telegram, Discord, PagerDuty, or arbitrary webhooks, with dedup and severity-based routing.

## What makes it different

- **Hash-chain reorg reconciliation.** Neither of the two open-source monitors it competes with (`ethereum-optimism/monitorism`, `OpenZeppelin/openzeppelin-monitor`) actually walks the parent-hash chain to detect reorgs; they track block numbers only. opsentry walks back to a common ancestor and emits both new-canonical and reverted events per confirmation policy.
- **OP-Stack-native template pack.** Prebuilt monitors for the system layer nobody else templates: `OptimismPortal` pause events, `SystemConfig` changes, `L2ToL1MessagePasser`. See [`rulesets/op-stack/`](rulesets/op-stack/).
- **Sourcify v2 ABI fetch.** Point at any verified contract address and opsentry loads its ABI automatically. Falls back to inline JSON or the built-in `erc20` when you need it.
- **Alerts-as-code.** Rules are YAML the CLI reads and writes. GitOps flow. No lock-in.
- **Apache-2.0.** Fork it, embed it, ship it in a proprietary stack. No copyleft.

## Quickstart

```bash
git clone https://github.com/nehemiyawicks/opsentry
cd opsentry
go build -o bin/opsentry ./cmd/opsentry
./bin/opsentry -config=config.example.yaml
```

That boots opsentry against Base and OP Mainnet public RPCs, monitors USDC on Base for transfers larger than $100k, and logs each alert to stderr in JSON. Swap the demo `type: log` receiver for `type: slack` + a real `slack://` URL to send to Slack instead.

Live evidence: 10s of runtime against Base pulls ~10 canonical blocks and surfaces every USDC transfer above the configured threshold.

## Docker

```bash
make compose-up
```

Runs opsentry + postgres + Prometheus from `deploy/docker-compose.yaml`. Prometheus is available at [http://localhost:9090](http://localhost:9090) and already scrapes opsentry's `/metrics`: search `opsentry_alerts_sent_total`, `opsentry_head_lag_blocks`, `opsentry_reorgs_seen_total`, etc.

## Reference rulesets

- [`rulesets/op-stack/`](rulesets/op-stack/): Portal, SystemConfig, L2ToL1MessagePasser
- [`rulesets/uniswap-v3/`](rulesets/uniswap-v3/): Factory PoolCreated
- [`rulesets/aave-v3/`](rulesets/aave-v3/): Pool LiquidationCall

Each is a runnable standalone config. Copy the `monitors:` block into your own config to combine them.

## Architecture

Five-stage pipeline, one goroutine per chain:

```
RPC ─▶ HeadTracker + Reconciler ─▶ LogFetcher ─▶ Decoder ─▶ RuleEvaluator ─▶ AlertManager ─▶ Router
       (poll latest,               (eth_getLogs   (ABI       (expr-lang)     (dedup by      (per receiver:
        hash-chain walkback         per canonical  decode)                    fingerprint,   log / slack /
        on reorg, SQLite            block)                                    SQLite         telegram /
        cursor persist)                                                       audit)         pagerduty /
                                                                                             webhook)
```

Rule expressions run in `expr-lang/expr` (sandboxed, non-Turing-complete). Alert delivery uses `containrrr/shoutrrr` (one URL → 15+ channels). Storage is SQLite via `modernc.org/sqlite` (cgo-free, single binary).

## Status

Early build. Mission milestones from [OP Governance Fund Mission Request #10293](https://gov.optimism.io/t/closed-governance-fund-mission-request-open-source-monitoring-alerting/10293):

- **M1** (SDK triggers alerts on Slack/Telegram/PagerDuty for ERC-20 transfers): **shipped**.
- **M2** (ruleset framework + Uniswap V3 & Aave V3 reference rulesets): **shipped**.
- **M3** (fully-built SDK + accompanying UI): SDK core done; UI in progress.

## Development

```bash
make test    # go test ./...
make race    # go test -race ./...
make vet     # go vet ./...
make lint    # golangci-lint run
make build   # produces bin/opsentry
make docker  # builds distroless image
```

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
