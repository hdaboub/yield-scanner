import { Address, BigInt, ethereum } from "@graphprotocol/graph-ts";
import {
  PairCreated,
  UniswapV2Factory as FactoryContract
} from "../generated/UniswapV2Factory/UniswapV2Factory";
import { UniswapV2Pair as UniswapV2PairTemplate } from "../generated/templates";
import { Pair, Token, V2SeedState } from "../generated/schema";
import { ERC20 } from "../generated/templates/UniswapV2Pair/ERC20";
import { UniswapV2Pair as PairContract } from "../generated/UniswapV2Factory/UniswapV2Pair";

const START_BLOCK = 24136052;
const SEED_CHUNK = 200;

function loadToken(addr: Address): void {
  let t = Token.load(addr);
  if (t == null) {
    t = new Token(addr);
    let erc20 = ERC20.bind(addr);

    let sym = erc20.try_symbol();
    if (!sym.reverted) t.symbol = sym.value;

    let nm = erc20.try_name();
    if (!nm.reverted) t.name = nm.value;

    let dec = erc20.try_decimals();
    if (!dec.reverted) t.decimals = dec.value;

    t.save();
  }
}

export function handlePairCreated(event: PairCreated): void {
  let p = Pair.load(event.params.pair);
  if (p == null) {
    p = new Pair(event.params.pair);
    p.token0 = event.params.token0;
    p.token1 = event.params.token1;
    p.createdAtTimestamp = event.block.timestamp;
    p.createdAtBlockNumber = event.block.number;
    p.reserve0 = BigInt.zero();
    p.reserve1 = BigInt.zero();
    p.lastSyncTimestamp = BigInt.zero();
    p.lastSyncBlockNumber = BigInt.zero();
    p.save();
  }

  loadToken(Address.fromBytes(event.params.token0));
  loadToken(Address.fromBytes(event.params.token1));

  UniswapV2PairTemplate.create(event.params.pair);
}

export function seedV2Pairs(block: ethereum.Block): void {
  if (block.number.lt(BigInt.fromI32(START_BLOCK))) return;

  let state = V2SeedState.load("v2");
  let factoryAddr = Address.fromString("0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac");
  let factory = FactoryContract.bind(factoryAddr);

  if (state == null) {
    state = new V2SeedState("v2");
    state.nextIndex = 0;
    state.total = factory.allPairsLength().toI32();
    state.lastBlock = block.number;
    state.save();
  }

  if (state.nextIndex >= state.total) return;

  let i = state.nextIndex;
  let end = i + SEED_CHUNK;
  if (end > state.total) end = state.total;

  for (; i < end; i++) {
    let pairAddr = factory.allPairs(BigInt.fromI32(i));

    // Create Pair entity if missing by reading token0/token1 from the pair contract
    let p = Pair.load(pairAddr);
    if (p == null) {
      let pc = PairContract.bind(pairAddr);
      let t0 = pc.token0();
      let t1 = pc.token1();

      p = new Pair(pairAddr);
      p.token0 = t0;
      p.token1 = t1;

      // We don't know real creation time without old logs; store seeding block as a placeholder
      p.createdAtTimestamp = block.timestamp;
      p.createdAtBlockNumber = block.number;

      p.reserve0 = BigInt.zero();
      p.reserve1 = BigInt.zero();
      p.lastSyncTimestamp = BigInt.zero();
      p.lastSyncBlockNumber = BigInt.zero();
      p.save();

      loadToken(t0);
      loadToken(t1);
    }

    UniswapV2PairTemplate.create(pairAddr);
  }

  state.nextIndex = i;
  state.lastBlock = block.number;
  state.save();
}
