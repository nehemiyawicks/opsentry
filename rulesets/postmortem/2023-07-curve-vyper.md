# Curve Reentrancy via Vyper Compiler Bug (July 2023)

**Loss:** ~$70M across multiple pools
**Chain:** Ethereum mainnet
**Root cause:** Vyper compiler versions 0.2.15, 0.2.16, and 0.3.0 emitted broken reentrancy-lock bytecode for `@nonreentrant('lock')` decorators.
**Attack date:** 2023-07-30

## What happened

Curve's stable-pair pools written in Vyper protected `remove_liquidity` with a `@nonreentrant('lock')` decorator, which the contract author believed prevented reentry via any function using the same lock name. Three older Vyper compiler versions had a bug that silently omitted the lock write on some code paths, leaving `remove_liquidity` reentrant.

The exploit called `remove_liquidity` on an affected pool, and in the ERC-20 callback (or ETH-transfer callback for ETH-side pools), reentered `remove_liquidity` a second time. Because the pool's internal accounting had already been partially updated on the first call but token transfers had not settled, the second call misread the pool's balance and let the attacker over-withdraw.

Affected pools included pETH/ETH, msETH/ETH, alETH/ETH, and CRV/ETH.

## The on-chain signature opsentry would have caught

The distinguishing pattern:

- **Two or more `RemoveLiquidity` (or `RemoveLiquidityImbalance`) events on the same pool in the same tx**, with matching `provider` addresses.
- Or equivalently, **two `Transfer` events out of the pool to the same address without an intervening `Deposit` or balance-adjusting event**.

Under normal use, a user calling `remove_liquidity` emits exactly one `RemoveLiquidity` event per tx. Nested emissions on the same pool are a strong reentrancy signal, and per-tx correlation is well within a monitor's reach: opsentry sees every log in tx order.

## Ruleset

See [`2023-07-curve-vyper.yaml`](./2023-07-curve-vyper.yaml). It watches the pETH/ETH pool (one of the affected pools) as historical documentation. To adapt for your pool:

1. Change `address` to any Vyper pool you care about (or any pool relying on Vyper's reentrancy decorator for safety).
2. Verify the pool uses Vyper > 0.3.0 (compiler versions with the fix). Ping the maintainer if you're unsure.
3. If your pool uses a different event name for withdrawals, update the ABI.
4. This rule uses per-event alerting; a fully same-tx-nested-event detection requires a "recent events window" feature currently on opsentry's roadmap. Until then, downstream correlation (Slack channel + human eyes) works.

## Postmortem sources

- Curve Finance post-mortem: https://mirror.xyz/curvefi.eth
- Vyper team disclosure: https://twitter.com/vyperlang/status/1685693459647614976
- Rekt News: https://rekt.news/curve-vyper-rekt/
