import { BigInt, Bytes } from "@graphprotocol/graph-ts";
import { Swap, Sync } from "../generated/templates/UniswapV2Pair/UniswapV2Pair";
import { V2Swap, Pair, PairHourData } from "../generated/schema";

const FEE_NUM = BigInt.fromI32(3);      // 0.30%
const FEE_DEN = BigInt.fromI32(1000);

function hourStart(ts: BigInt): i32 {
  return (ts.toI32() / 3600) * 3600;
}

function loadHour(pairAddr: Bytes, ts: BigInt): PairHourData {
  let h = hourStart(ts);
  let id = pairAddr.toHexString() + "-" + h.toString();
  let e = PairHourData.load(id);
  if (e == null) {
    e = new PairHourData(id);
    e.pair = pairAddr;
    e.hourStartUnix = h;
    e.volume0In = BigInt.zero();
    e.volume1In = BigInt.zero();
    e.volume0Out = BigInt.zero();
    e.volume1Out = BigInt.zero();
    e.fee0 = BigInt.zero();
    e.fee1 = BigInt.zero();
    e.reserve0 = BigInt.zero();
    e.reserve1 = BigInt.zero();
    e.swapCount = 0;
  }
  return e as PairHourData;
}

export function handleV2Swap(event: Swap): void {
  let id = event.transaction.hash.toHexString() + "-" + event.logIndex.toString();
  let s = new V2Swap(id);

  s.pair = event.address;
  s.sender = event.params.sender;
  s.to = event.params.to;
  s.amount0In = event.params.amount0In;
  s.amount1In = event.params.amount1In;
  s.amount0Out = event.params.amount0Out;
  s.amount1Out = event.params.amount1Out;
  s.blockNumber = event.block.number;
  s.timestamp = event.block.timestamp;
  s.txHash = event.transaction.hash;
  s.save();

  let h = loadHour(event.address, event.block.timestamp);

  h.volume0In = h.volume0In.plus(event.params.amount0In);
  h.volume1In = h.volume1In.plus(event.params.amount1In);
  h.volume0Out = h.volume0Out.plus(event.params.amount0Out);
  h.volume1Out = h.volume1Out.plus(event.params.amount1Out);

  if (event.params.amount0In.gt(BigInt.zero())) {
    h.fee0 = h.fee0.plus(event.params.amount0In.times(FEE_NUM).div(FEE_DEN));
  }
  if (event.params.amount1In.gt(BigInt.zero())) {
    h.fee1 = h.fee1.plus(event.params.amount1In.times(FEE_NUM).div(FEE_DEN));
  }

  h.swapCount = h.swapCount + 1;
  let p = Pair.load(event.address);
  if (p != null) {
    h.reserve0 = p.reserve0;
    h.reserve1 = p.reserve1;
  }
  h.save();
}

export function handleV2Sync(event: Sync): void {
  let p = Pair.load(event.address);
  if (p != null) {
    p.reserve0 = event.params.reserve0;
    p.reserve1 = event.params.reserve1;
    p.lastSyncTimestamp = event.block.timestamp;
    p.lastSyncBlockNumber = event.block.number;
    p.save();
  }

  let h = loadHour(event.address, event.block.timestamp);
  h.reserve0 = event.params.reserve0;
  h.reserve1 = event.params.reserve1;
  h.save();
}
