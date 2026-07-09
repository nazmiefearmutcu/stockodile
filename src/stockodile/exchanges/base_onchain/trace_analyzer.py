import logging
from typing import Any

log = logging.getLogger(__name__)

FLASHLOAN_SELECTORS = {
    "0x49077656",  # Uniswap V3 flash
    "0xe00fcfbf",  # Balancer flashLoan
    "0x5cffe9de",  # ERC3156 flashLoan
    "0xab305a4e",  # Aave V3 flashLoan
    "0x43b2fbe0",  # Aave V3 flashLoanSimple
    "0x0f6b45b0",  # Aave V2 flashLoan
}

SWAP_SELECTORS = {
    "0x128acb08": "uniswap_v3",
    "0x022c0d9f": "uniswap_v2_or_aerodrome"
}

class TraceAnalyzer:
    def analyze_trace(self, tx_hash: str, trace_result: dict[str, Any]) -> dict[str, Any]:
        """
        Parses structLogs or parity call-trace to identify:
        - flashloans: bool
        - reentrancy_detected: bool
        - internal_swaps: list[dict]
        """
        flashloans = False
        reentrancy_detected = False
        internal_swaps = []
        
        # Check trace format
        # Geth structLogs style
        if "structLogs" in trace_result:
            struct_logs = trace_result["structLogs"]
            depth_map = {}
            active_calls: dict[str, list[int]] = {} # address -> list of depths
            
            for log_item in struct_logs:
                depth = log_item.get("depth", 1)
                op = log_item.get("op", "")
                
                # Reentrancy & Internal Swaps detection in Geth CALLs
                if op in ("CALL", "DELEGATECALL", "CALLCODE", "STATICCALL") and "stack" in log_item:
                    stack = log_item["stack"]
                    if len(stack) >= 2:
                        # target is stack[-2], stack items are 32-byte hex strings
                        target = "0x" + stack[-2][-40:].lower()
                        next_depth = depth + 1
                        depth_map[next_depth] = target
                        
                        # Reentrancy check: is target already in depth_map for any parent depth?
                        if op != "STATICCALL":
                            for d in range(1, next_depth):
                                if depth_map.get(d) == target:
                                    reentrancy_detected = True
                                    break
                                    
                        # Flashloan check via selector if input data or stack contains it
                        # In Geth, sometimes stack has input parameters depending
                        # on tracing configuration.
                        # We can also check if the tx's top-level input contains flashloan selector.
                        
                # If we see any of the flashloan selectors in stack or memory
                # Let's do a search on stack values for selectors
                if "stack" in log_item:
                    for val in log_item["stack"]:
                        # check if val starts with selector (val is 32-byte hex)
                        for selector in FLASHLOAN_SELECTORS:
                            if val.startswith(selector[2:]) or val.startswith(
                                "00" * 28 + selector[2:]
                            ):
                                flashloans = True
                                
        # Parity/Erigon call-trace style (usually a list of traces or {"result": [...]})
        else:
            traces = (
                trace_result
                if isinstance(trace_result, list)
                else trace_result.get("result", [])
            )
            if isinstance(traces, list):
                active_calls = {} # address -> list of traceAddress
                
                for trace in traces:
                    trace_type = trace.get("type")
                    if trace_type == "call":
                        action = trace.get("action", {})
                        from_addr = action.get("from", "")
                        to_addr = action.get("to", "")
                        input_data = action.get("input", "")
                        call_type = action.get("callType", "")
                        trace_addr = trace.get("traceAddress", [])
                        
                        # Reentrancy check
                        if to_addr and call_type != "staticcall":
                            to_addr_lower = to_addr.lower()
                            if to_addr_lower in active_calls:
                                for ancestor_addr in active_calls[to_addr_lower]:
                                    if (
                                        len(trace_addr) > len(ancestor_addr)
                                        and trace_addr[: len(ancestor_addr)]
                                        == ancestor_addr
                                    ):
                                        reentrancy_detected = True
                                        break
                            if to_addr_lower not in active_calls:
                                active_calls[to_addr_lower] = []
                            active_calls[to_addr_lower].append(trace_addr)
                            
                        # Flashloan check
                        if input_data:
                            selector = input_data[:10].lower()
                            if selector in FLASHLOAN_SELECTORS:
                                flashloans = True
                                
                        # Internal swaps check
                        if input_data:
                            selector = input_data[:10].lower()
                            if selector in SWAP_SELECTORS:
                                protocol = SWAP_SELECTORS[selector]
                                internal_swaps.append({
                                    "pool": to_addr,
                                    "protocol": protocol,
                                    "sender": from_addr,
                                    "selector": selector
                                })
                                
        return {
            "flashloans": flashloans,
            "reentrancy_detected": reentrancy_detected,
            "internal_swaps": internal_swaps
        }
