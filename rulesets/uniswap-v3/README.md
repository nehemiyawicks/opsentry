# Uniswap V3 ruleset pack

Monitors for the Uniswap V3 factory and pools. The mission spec called this pack out by name as an M2 deliverable.

## Files

### `factory-base.yaml`: Factory on Base

Watches `UniswapV3Factory` at `0x33128a8fC17869897dcE68Ed026d694621f6FDfD` on Base. Fires on:

- **`PoolCreated`** → severity `info`. Emits one event per new pool. Useful as a discovery feed for indexers and as a canary for spam-token pool creation.

Uniswap V3 Factory addresses are consistent across most EVM chains at this same address (Factory was deployed via CREATE2). If you want the factory on OP Mainnet, change the `chain` and `chain_id`; the address is the same.

## Extending

To monitor a specific pool (e.g., USDC / WETH 0.05%), add a second monitor block pointing at that pool's address with the Uniswap V3 Pool ABI. The pool contract emits `Swap`, `Mint`, `Burn`, `Flash`, `Collect`. A useful rule shape:

```yaml
- when: 'event.name == "Swap" && (event.params.amount0 > 1e12 || event.params.amount1 > 1e18)'
  severity: high
  receivers: [slack]
```

Note that pool amounts are in token-native decimals (USDC 6, WETH 18) so thresholds are asset-specific.

## Address verification

Uniswap V3 canonical deployment addresses live at [docs.uniswap.org](https://docs.uniswap.org/contracts/v3/reference/deployments).
