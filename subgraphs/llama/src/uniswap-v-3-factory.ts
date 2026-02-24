import { PoolCreated } from "../generated/UniswapV3Factory/UniswapV3Factory";
import { UniswapV3Pool } from "../generated/templates";

export function handlePoolCreated(event: PoolCreated): void {
  UniswapV3Pool.create(event.params.pool);
}
