from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from stockodile.schema.records import BookSnapshot, Quote, Record, Trade

log = logging.getLogger(__name__)

EXCHANGE = "base_onchain"

def normalize_onchain_update(
    msg: dict[str, Any], local_ts: int, exchange: str = EXCHANGE
) -> Iterable[Record]:
    """Normalize on-chain pool updates.
    
    The input msg has the structure:
    {
        "type": "onchain_update",
        "block": int,
        "pool": str, (e.g. "cbBTC-USDC")
        "pool_type": "uniswap_v3" | "aerodrome_v2",
        "timestamp": int,
        "state": { ... },
        "swaps": [ ... ]
    }
    """
    pool_name = msg["pool"]
    pool_type = msg["pool_type"]
    state = msg["state"]
    swaps = msg.get("swaps", [])
    block = msg["block"]
    
    # 1. Parse swaps into Trade records
    for sw in swaps:
        try:
            float(sw["price"])
            float(sw["amount"])
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid swap numeric value: {e}") from e
            
        yield Trade(
            provider=exchange,
            symbol=f"{exchange}:{pool_name}",
            symbol_raw=pool_name,
            source_ts=sw["timestamp"] * 1_000_000_000,
            local_ts=local_ts,
            id=f"{sw['tx_hash']}-{sw['log_index']}",
            price=float(sw["price"]),
            size=float(sw["amount"]),
        )
        
    # 2. Parse state into BookSnapshot and BookTicker
    import math

    def safe_int(val: Any, default: int) -> int:
        if val is None:
            return default
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def safe_float(val: Any, default: float = 0.0) -> float:
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    price = state["price"]
    if price is None:
        raise TypeError("price cannot be None")
    if isinstance(price, bool):
        raise TypeError("price cannot be boolean")
    try:
        price_val = float(price)
    except (ValueError, TypeError) as e:
        raise TypeError(f"price is invalid: {price}") from e
        
    if price_val <= 0 or math.isnan(price_val) or math.isinf(price_val):
        return
    price = price_val
        
    reserve_token0_raw = state.get("reserve0", 0.0)
    reserve_token1_raw = state.get("reserve1", 0.0)
    if reserve_token0_raw is None or reserve_token1_raw is None:
        raise TypeError("reserves cannot be None")
    if isinstance(reserve_token0_raw, bool) or isinstance(reserve_token1_raw, bool):
        raise TypeError("reserves cannot be boolean")
        
    reserve_token0 = safe_float(reserve_token0_raw, 0.0)
    reserve_token1 = safe_float(reserve_token1_raw, 0.0)
    
    if (
        math.isnan(reserve_token0)
        or math.isinf(reserve_token0)
        or math.isnan(reserve_token1)
        or math.isinf(reserve_token1)
    ):
        return
        
    if reserve_token0 < 0:
        reserve_token0 = 0.0
    if reserve_token1 < 0:
        reserve_token1 = 0.0
    
    bids = []
    asks = []
    
    decimals0 = safe_int(state.get("decimals0"), 8 if "btc" in pool_name.lower() else 18)
    decimals1 = safe_int(state.get("decimals1"), 18)
    decimals0 = max(0, min(decimals0, 36))
    decimals1 = max(0, min(decimals1, 36))
    
    is_flipped_raw = state.get("is_flipped", False)
    if isinstance(is_flipped_raw, str):
        is_flipped = is_flipped_raw.lower() in ("true", "1")
    else:
        is_flipped = bool(is_flipped_raw)

    use_active_v3 = False
    if pool_type == "uniswap_v3" and "liquidity" in state:
        liq_val = state.get("liquidity")
        if liq_val is not None:
            try:
                liquidity = float(liq_val)
                if liquidity > 0:
                    use_active_v3 = True
            except (ValueError, TypeError):
                pass

    if use_active_v3:
        liquidity = float(state["liquidity"])
        if not math.isfinite(liquidity) or liquidity < 0:
            return
        tick_spacing = safe_int(state.get("tickSpacing") or state.get("tick_spacing"), 10)
        tick_spacing = max(tick_spacing, 1)
        
        # Calculate active tick
        dec_diff = decimals0 - decimals1
        try:
            if not is_flipped:
                price_ratio = price / (10 ** dec_diff)
            else:
                price_ratio = (10 ** dec_diff) / price
            
            if math.isinf(price_ratio) or math.isnan(price_ratio):
                return
                
            if price_ratio > 0:
                tick = math.log(price_ratio) / math.log(1.0001)
            else:
                tick_raw = state.get("tick")
                if tick_raw is None:
                    tick = 0.0
                else:
                    tick = safe_float(tick_raw, 0.0)
            
            if math.isnan(tick) or math.isinf(tick):
                return
        except Exception:
            tick_raw = state.get("tick")
            if tick_raw is None:
                tick = 0.0
            else:
                tick = safe_float(tick_raw, 0.0)
                if math.isnan(tick) or math.isinf(tick):
                    return
            
        def get_price_at_tick(t: float, flipped: bool, dec0: int, dec1: int) -> float:
            dec_diff = dec0 - dec1
            try:
                if not flipped:
                    return float((1.0001 ** t) * (10 ** dec_diff))
                else:
                    return float((1.0001 ** (-t)) * (10 ** dec_diff))
            except Exception:
                return 0.0
        
        def safe_cap(val: float) -> float:
            try:
                if math.isnan(val) or math.isinf(val):
                    return val
            except Exception:
                pass
            return max(val, 0.0001)

        # Calculate 5 levels of bids and asks
        for i in range(1, 6):
            if not is_flipped:
                ask_t1 = tick + (i - 1) * tick_spacing
                ask_t2 = tick + i * tick_spacing
                bid_t1 = tick - i * tick_spacing
                bid_t2 = tick - (i - 1) * tick_spacing
            else:
                ask_t1 = tick - i * tick_spacing
                ask_t2 = tick - (i - 1) * tick_spacing
                bid_t1 = tick + (i - 1) * tick_spacing
                bid_t2 = tick + i * tick_spacing
            
            ask_px = get_price_at_tick(
                ask_t2 if not is_flipped else ask_t1, is_flipped, decimals0, decimals1
            )
            bid_px = get_price_at_tick(
                bid_t1 if not is_flipped else bid_t2, is_flipped, decimals0, decimals1
            )
            
            try:
                sqrt_ask1 = 1.0001 ** (ask_t1 / 2.0)
                sqrt_ask2 = 1.0001 ** (ask_t2 / 2.0)
                sqrt_bid1 = 1.0001 ** (bid_t1 / 2.0)
                sqrt_bid2 = 1.0001 ** (bid_t2 / 2.0)
            except OverflowError:
                return
            
            if not is_flipped:
                try:
                    ask_sz = (liquidity * (1.0 / sqrt_ask1 - 1.0 / sqrt_ask2)) / (10 ** decimals0)
                    bid_sz = (
                        ((liquidity * (sqrt_bid2 - sqrt_bid1)) / (10 ** decimals1)) / bid_px
                        if bid_px > 0 else 0.0
                    )
                except ZeroDivisionError:
                    return
            else:
                try:
                    ask_sz = (liquidity * (sqrt_ask2 - sqrt_ask1)) / (10 ** decimals1)
                    bid_sz = (
                        (
                            (liquidity * (1.0 / sqrt_bid1 - 1.0 / sqrt_bid2))
                            / (10 ** decimals0)
                        )
                        / bid_px
                        if bid_px > 0
                        else 0.0
                    )
                except ZeroDivisionError:
                    return
            
            # Discard updates if calculated prices or sizes result in NaN or Inf
            if not (
                math.isfinite(ask_px)
                and math.isfinite(ask_sz)
                and math.isfinite(bid_px)
                and math.isfinite(bid_sz)
            ):
                return
            
            bids.append((bid_px, safe_cap(bid_sz)))
            asks.append((ask_px, safe_cap(ask_sz)))
    else:
        # Fallback for Uniswap V3 without liquidity OR Aerodrome V2
        def safe_cap(val: float) -> float:
            try:
                if math.isnan(val) or math.isinf(val):
                    return val
            except Exception:
                pass
            return max(val, 0.0001)

        base_reserve = reserve_token1 if is_flipped else reserve_token0
        
        for i in range(1, 6):
            spread_prev = 0.0005 * (i - 1)
            spread_curr = 0.0005 * i
            
            bid_px = price * (1.0 - spread_curr)
            ask_px = price * (1.0 + spread_curr)
            
            # Use constant product formulas
            try:
                ask_sz = base_reserve * (
                    1.0 / math.sqrt(1.0 + spread_prev) - 1.0 / math.sqrt(1.0 + spread_curr)
                )
                bid_sz = base_reserve * (
                    1.0 / math.sqrt(1.0 - spread_curr) - 1.0 / math.sqrt(1.0 - spread_prev)
                )
            except (ZeroDivisionError, ValueError):
                return
            
            # Discard updates if calculated prices or sizes result in NaN or Inf
            if not (
                math.isfinite(ask_px)
                and math.isfinite(ask_sz)
                and math.isfinite(bid_px)
                and math.isfinite(bid_sz)
            ):
                return
            
            bids.append((bid_px, safe_cap(bid_sz)))
            asks.append((ask_px, safe_cap(ask_sz)))
            
    # Best levels
    bid_px, bid_sz = bids[0]
    ask_px, ask_sz = asks[0]
    
    yield Quote(
        provider=exchange,
        symbol=f"{exchange}:{pool_name}",
        symbol_raw=pool_name,
        local_ts=local_ts,
        bid_px=bid_px,
        bid_sz=bid_sz,
        ask_px=ask_px,
        ask_sz=ask_sz,
        source_ts=msg["timestamp"] * 1_000_000_000,
    )
    
    yield BookSnapshot(
        provider=exchange,
        symbol=f"{exchange}:{pool_name}",
        symbol_raw=pool_name,
        local_ts=local_ts,
        bids=bids,
        asks=asks,
        depth=len(bids),
        source_ts=msg["timestamp"] * 1_000_000_000,
        sequence_id=block,
        is_snapshot=True
    )
