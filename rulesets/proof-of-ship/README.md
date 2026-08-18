# proof-of-ship

Rulesets watching live production apps built for [Celo Proof of Ship](https://www.celopg.eco/programs/proof-of-ship-s1) and similar monthly builder programs. These are here to prove opsentry works against consumer-app deployments on non-OP-Stack chains (Celo, in this folder's case), not just L2 bridge contracts.

Anyone shipping a Proof of Ship app can send a PR adding their own contract here.

## What's here

- **`splitpay-celo.yaml`** — [splitpay](https://github.com/nehemiyawicks/splitpay), a Splitwise-style group expense splitter live at [`0x2979d1808024bd81eaba87942d79f7b2168e39c4`](https://celoscan.io/address/0x2979d1808024bd81eaba87942d79f7b2168e39c4) on Celo mainnet. Watches `GroupCreated`, `ExpenseAdded`, `Settled` and logs each.

## Adding your app

1. Copy `splitpay-celo.yaml` and replace the `chain`, `address`, `abi`, and event rules.
2. If your chain isn't Celo mainnet, adjust the `chains:` block (chain_id, RPC, block_time_ms).
3. Run `go test ./internal/config/...` from repo root. `TestRulesetsParse` will load your file through opsentry's real config loader and catch any syntax mistakes.
4. Open a PR.
