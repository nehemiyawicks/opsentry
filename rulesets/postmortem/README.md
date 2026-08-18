# postmortem

Rulesets that would have caught known past hacks. Each entry is a pair:

- `<slug>.md`: short postmortem explaining the attack, root cause, and the on-chain signature that opsentry would have detected
- `<slug>.yaml`: the runnable opsentry ruleset targeting that signature

The rulesets are **pattern templates**, not deployable-as-is monitors for the original victim contracts (many of which are dead or forked). The intended use is:

1. **Read the postmortem** to understand the class of attack.
2. **Copy the yaml** and adapt the `address`, `chain`, and event/state field names to your own contracts that share the pattern.
3. **Run it** as part of your production monitoring config.

Contributions welcome. If you've written a postmortem for an incident with a clear on-chain signature, open a PR with the pair.

## What's here

- **`2023-03-euler.md` / `.yaml`**: Euler Finance donation-attack + violent liquidation (~$197M). Signature: `donateToReserves` immediately followed by `Liquidation` within the same tx sender window.
- **`2022-08-nomad.md` / `.yaml`**: Nomad Bridge signature verification bypass (~$190M). Signature: cascading `Process` calls from many EOAs draining the bridge in minutes.
- **`2023-07-curve-vyper.md` / `.yaml`**: Curve reentrancy via Vyper compiler bug (~$70M). Signature: nested `RemoveLiquidity` events on the affected pools within a single tx.

## How to add a new one

1. Copy an existing pair as a starting point.
2. Write the postmortem: what happened in one paragraph, root cause in one paragraph, the on-chain signature opsentry would have watched for in one paragraph.
3. Write the ruleset: chain block, receivers block, and monitors block with rules that fire on the signature.
4. Run `go test ./internal/config/...` from repo root to make sure `TestRulesetsParse` accepts the new yaml.
5. Open a PR.
