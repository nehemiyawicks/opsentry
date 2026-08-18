# opsentry rulesets

Reference monitor configurations you can copy into your `config.yaml`, or run standalone.

Each subfolder covers one protocol or system layer. Every `.yaml` file here is a complete, runnable opsentry config: the `chains`, `receivers`, and `monitors` blocks are all present. Point opsentry at one of them with `-config` and it will start monitoring the corresponding contracts.

For your real deployment, take the `monitors:` blocks you want, drop them into your own config, and share your `chains:` and `receivers:` sections across all of them.

## What's here

- **`op-stack/`** — Superchain / OP-Stack system contracts. Portal pause events, SystemConfig changes, L2 withdrawal message passer, cross-domain messenger. Sourced from the invariants in [`ethereum-optimism/monitorism`](https://github.com/ethereum-optimism/monitorism).
- **`uniswap-v3/`** — Uniswap V3 factory and pools.
- **`aave-v3/`** — Aave V3 lending pool.
- **`tokens/`** — High-volume ERC-20 large-transfer alerts (USDC, WETH on Base). Copy-adapt for any ERC-20.
- **`proof-of-ship/`** — Live production apps built for [Celo Proof of Ship](https://www.celopg.eco/programs/proof-of-ship-s1) and similar builder programs. Proves opsentry works against consumer-app deployments on non-OP-Stack chains. Open to PRs from other Proof of Ship builders.
- **`postmortem/`** — Rulesets that would have caught known past hacks. Each entry pairs a short postmortem writeup with a runnable ruleset template. Meant as an educational corpus and a starting point for teams wiring detection for the same class of attack. Open to PRs.

## Verifying addresses

Contract addresses in these rulesets come from official sources (superchain-registry, protocol docs) but you should verify them against a trusted source before relying on them in production. A wrong address means you'll silently miss the events you care about.

## Verifying rules

Run `go test ./internal/config/...` before committing changes to a ruleset. The `TestRulesetsParse` integration test loads every `.yaml` under this directory through the same config loader opsentry uses at boot, so syntax errors and unknown receiver references fail in CI, not production.
