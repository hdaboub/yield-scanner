import { Swap } from "../generated/templates/UniswapV3Pool/UniswapV3Pool";
import { V3Swap } from "../generated/schema";

export function handleV3Swap(event: Swap): void {
  let id = event.transaction.hash.toHexString() + "-" + event.logIndex.toString();
  let e = new V3Swap(id);

  e.pool = event.address;
  e.sender = event.params.sender;
  e.recipient = event.params.recipient;
  e.amount0 = event.params.amount0;
  e.amount1 = event.params.amount1;
  e.sqrtPriceX96 = event.params.sqrtPriceX96;
  e.liquidity = event.params.liquidity;
  e.tick = event.params.tick;
  e.blockNumber = event.block.number;
  e.timestamp = event.block.timestamp;
  e.txHash = event.transaction.hash;

  e.save();
}
