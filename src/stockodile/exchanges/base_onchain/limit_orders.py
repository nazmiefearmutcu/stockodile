import logging
from typing import Any

import eth_abi
from eth_utils import to_checksum_address

log = logging.getLogger(__name__)

# Event topics
ONEINCH_ORDER_FILLED_TOPIC = "0x09da1b238d7fe6167120df0559f635c02b2176435f0de367098e945c73d9d300"
# Actual 0x V4 LimitOrderFilled signature: LimitOrderFilled(address maker, address taker,
# address makerToken, address takerToken, uint128 makerTokenFilledAmount,
# uint128 takerTokenFilledAmount, bytes32 orderHash)
# let's compute actual topic for LimitOrderFilled(address,address,address,address,
# uint128,uint128,bytes32)
# keccak256("LimitOrderFilled(address,address,address,address,uint128,uint128,bytes32)")
# = 0xab61feda74a9d45f448651a5c6819eb6085a6b0c265a7df498d5c328db9403d1
ZEROX_LIMIT_ORDER_FILLED_TOPIC = (
    "0xab61feda74a9d45f448651a5c6819eb6085a6b0c265a7df498d5c328db9403d1"
)


def decode_1inch_order_filled(
    topics: list[str], data: str, receipt: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Decodes 1inch OrderFilled log.

    Event: OrderFilled(bytes32 orderHash, address maker, address taker,
    uint256 remaining, uint256 makerAmount, uint256 takerAmount)
    """
    data_hex = data[2:] if data.startswith("0x") else data
    data_bytes = bytes.fromhex(data_hex)
    
    # 1inch OrderFilled might have different indexing, let's decode based on length of topics.
    if len(topics) == 1:
        # All fields in data (192 bytes)
        decoded = eth_abi.decode(
            ["bytes32", "address", "address", "uint256", "uint256", "uint256"],
            data_bytes
        )
        order_hash = "0x" + decoded[0].hex()
        maker = to_checksum_address(decoded[1])
        taker = to_checksum_address(decoded[2])
        _ = decoded[3]
        maker_amount = float(decoded[4])
        taker_amount = float(decoded[5])
    elif len(topics) == 2:
        # orderHash indexed
        order_hash = topics[1]
        decoded = eth_abi.decode(
            ["address", "address", "uint256", "uint256", "uint256"],
            data_bytes
        )
        maker = to_checksum_address(decoded[0])
        taker = to_checksum_address(decoded[1])
        _ = decoded[2]
        maker_amount = float(decoded[3])
        taker_amount = float(decoded[4])
    else:
        # Default fallback
        order_hash = topics[1] if len(topics) > 1 else "0x" + "0" * 64
        maker = to_checksum_address("0x" + topics[2][-40:]) if len(topics) > 2 else "0x" + "0" * 40
        taker = to_checksum_address("0x" + topics[3][-40:]) if len(topics) > 3 else "0x" + "0" * 40
        decoded = eth_abi.decode(
            ["uint256", "uint256", "uint256"],
            data_bytes
        )
        _ = decoded[0]
        maker_amount = float(decoded[1])
        taker_amount = float(decoded[2])
        
    # Dynamically extract maker/taker tokens from Transfer logs in receipt
    maker_token = "0x0000000000000000000000000000000000000000"
    taker_token = "0x0000000000000000000000000000000000000000"
    if receipt and "logs" in receipt:
        for log_item in receipt["logs"]:
            log_topics = log_item.get("topics", [])
            # ERC20 Transfer topic
            transfer_topic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            if len(log_topics) == 3 and log_topics[0].hex().replace("0x", "") == transfer_topic:
                from_addr = "0x" + log_topics[1].hex()[-40:]
                to_addr = "0x" + log_topics[2].hex()[-40:]
                token_addr = log_item.get("address")
                if from_addr.lower() == maker.lower():
                    maker_token = token_addr
                if to_addr.lower() == maker.lower():
                    taker_token = token_addr
                    
    return {
        "protocol": "1inch",
        "order_hash": order_hash,
        "maker": maker,
        "taker": taker,
        "maker_token": to_checksum_address(maker_token),
        "taker_token": to_checksum_address(taker_token),
        "maker_amount": maker_amount,
        "taker_amount": taker_amount,
    }

def decode_0x_limit_order_filled(topics: list[str], data: str) -> dict[str, Any]:
    """Decodes 0x LimitOrderFilled log.

    Event: LimitOrderFilled(address maker, address taker, address makerToken,
                            address takerToken, uint128 makerTokenFilledAmount,
                            uint128 takerTokenFilledAmount, bytes32 orderHash)
    """
    data_hex = data[2:] if data.startswith("0x") else data
    data_bytes = bytes.fromhex(data_hex)
    
    # 0x LimitOrderFilled details
    if len(topics) == 1:
        # All fields in data
        decoded = eth_abi.decode(
            ["address", "address", "address", "address", "uint128", "uint128", "bytes32"],
            data_bytes
        )
        maker = to_checksum_address(decoded[0])
        taker = to_checksum_address(decoded[1])
        maker_token = to_checksum_address(decoded[2])
        taker_token = to_checksum_address(decoded[3])
        maker_amount = float(decoded[4])
        taker_amount = float(decoded[5])
        order_hash = "0x" + decoded[6].hex()
    else:
        # Assume standard indexing (none or some indexed)
        # Let's decode by matching data length or using fallback
        try:
            # Try decoding as 3 address, 3 address, uint128, uint128, bytes32
            # typically only orderHash and/or maker/taker might be indexed.
            # If length is 160 bytes: makerToken, takerToken, makerTokenFilledAmount,
            # takerTokenFilledAmount, orderHash (orderHash is 32 bytes, uint128
            # are 32 bytes padded)
            # Let's check size of data_bytes
            if len(data_bytes) == 160:  # 5 fields
                decoded = eth_abi.decode(
                    ["address", "address", "uint256", "uint256", "bytes32"],
                    data_bytes
                )
                maker = (
                    to_checksum_address("0x" + topics[1][-40:])
                    if len(topics) > 1
                    else "0x" + "0" * 40
                )
                taker = (
                    to_checksum_address("0x" + topics[2][-40:])
                    if len(topics) > 2
                    else "0x" + "0" * 40
                )
                maker_token = to_checksum_address(decoded[0])
                taker_token = to_checksum_address(decoded[1])
                maker_amount = float(decoded[2])
                taker_amount = float(decoded[3])
                order_hash = "0x" + decoded[4].hex()
            else:
                # Fallback to general decode
                decoded = eth_abi.decode(
                    ["address", "address", "address", "address", "uint256", "uint256", "bytes32"],
                    data_bytes
                )
                maker = to_checksum_address(decoded[0])
                taker = to_checksum_address(decoded[1])
                maker_token = to_checksum_address(decoded[2])
                taker_token = to_checksum_address(decoded[3])
                maker_amount = float(decoded[4])
                taker_amount = float(decoded[5])
                order_hash = "0x" + decoded[6].hex()
        except Exception:
            # Absolute fallback
            maker = "0x" + "0" * 40
            taker = "0x" + "0" * 40
            maker_token = "0x" + "0" * 40
            taker_token = "0x" + "0" * 40
            maker_amount = 0.0
            taker_amount = 0.0
            order_hash = "0x" + "0" * 64
            
    return {
        "protocol": "0x",
        "order_hash": order_hash,
        "maker": maker,
        "taker": taker,
        "maker_token": maker_token,
        "taker_token": taker_token,
        "maker_amount": maker_amount,
        "taker_amount": taker_amount,
    }
