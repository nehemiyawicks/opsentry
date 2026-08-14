# opsentry

Open-source contract monitoring and alerting for the OP Stack. Self-hostable, Apache-2.0.

## What it is

A self-hostable service that watches contracts on Base, OP Mainnet, and any OP-Stack rollup. Rules are YAML: event matches, function-call filters, thresholds, and `eth_call` invariants. Alerts fan out to Slack, Telegram, Discord, PagerDuty, or arbitrary webhooks, with dedup and severity-based routing.

## What makes it different

- **OP-Stack-native template pack.** Prebuilt monitors for the system layer nobody else templates: sequencer liveness, fault-proof and dispute-game activity, `SystemConfig` changes, `OptimismPortal` / `CrossDomainMessenger` / `StandardBridge` flows, fee-vault balances, Guardian and proxy-admin role changes. Keyed by chain ID, so it runs on Base, OP Mainnet, and any OP-Stack rollup out of the box.
- **A UI.** The thing every generic monitor makes you hand-write YAML for.
- **Apache-2.0.** Fork it, embed it, ship it in a proprietary stack. No copyleft.
- **`eth_call` invariants.** View-function and state assertions: *"TVL must not drop >X%/block"*, *"owner must equal the Safe"*, *"supply monotonic"*.
- **Alerts-as-code.** Rules are YAML the UI reads and writes. GitOps flow works. No lock-in to the dashboard.

## Status

Early build. Not yet packaged for install; watch the repo.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
