import asyncio, os
import json
import numpy as np
import websockets
import logging
import sys
import time

# =====================================================================
# SYSTEM STATIC RISK THRESHOLDS (GLOBAL SCOPE)
# =====================================================================
MARKET_CAP_LIMIT = 9_999_999_999.0
SHARE_FLOAT_LIMIT = 2_000_000.0
SHORT_INTEREST_MIN = 0.0
PREMIUM_MIN = 0.0
PREMIUM_MAX = 9_999_999_999.0
TRIGGER_MULTIPLIER = 3.0

# =====================================================================
# CORE QUANTITATIVE COMPUTATION ENGINE
# =====================================================================
class PhantomSqueezeEngine:
    def __init__(self):
        self.active_tracked_alerts = {}
        self.ticker_registry = []
        self.ticker_to_id = {}

    def _get_ticker_id(self, ticker_str: str) -> int:
        """Maps alphanumeric ticker strings to raw memory integers inline."""
        if ticker_str not in self.ticker_to_id:
            new_id = len(self.ticker_registry)
            self.ticker_to_id[ticker_str] = new_id
            self.ticker_registry.append(ticker_str)
            return new_id
        return self.ticker_to_id[ticker_str]

    def run_vectorized_pipeline(self, raw_market_inputs):
        n = len(raw_market_inputs)
        if n == 0: 
            return None
        
        # Localize method mapping to eliminate attribute tracking overhead in loop
        get_id = self._get_ticker_id
        
        # STEP 1: CONVERT BLOCKS WITH ZERO MANUAL INDEX LOOPS
        try:
            raw_matrix_data = [
                [
                    get_id(asset.get('ticker', '')),
                    float(asset.get('market_cap', 9_999_999_999.0)),
                    float(asset.get('share_float', 2_000_000.0)),
                    float(asset.get('short_interest_pct', 0.0)),
                    float(asset.get('premium', 0.0)),
                    float(asset.get('vol_ratio', 0.0)),
                    float(asset.get('vol', 0.0)),
                    float(asset.get('delta', 0.50))
                ]
                for asset in raw_market_inputs
            ]
        except Exception:
            return None

        # Instantiate contiguous C-level memory layout matrix instantly
        matrix = np.array(raw_matrix_data, dtype=np.float64)

        # STEP 2: APPLY HARDWARE-LEVEL BITWISE MACHINE MASKS
        mask = (
            (matrix[:, 1] < MARKET_CAP_LIMIT) &
            (matrix[:, 2] < SHARE_FLOAT_LIMIT) &
            (matrix[:, 3] >= SHORT_INTEREST_MIN) &
            (matrix[:, 4] >= PREMIUM_MIN) &
            (matrix[:, 4] <= PREMIUM_MAX) &
            (matrix[:, 5] >= TRIGGER_MULTIPLIER)
        )
        
        # Slice matrix to isolate only anomalous rows
        anomalies = matrix[mask]

        if anomalies.size == 0:
            return None

        # STEP 3: HIGH-SPEED VECTORIZED LIQUIDITY MATH (Your Core Logic)
        total_underlying_shares = anomalies[:, 6] * 100.0
        forced_mm_share_demand = total_underlying_shares * anomalies[:, 7]
        
        scaling_multiplier = 2_000_000.0 / anomalies[:, 2]
        final_calculated_demand = forced_mm_share_demand * scaling_multiplier
        float_impact_percentage = (final_calculated_demand / anomalies[:, 2]) * 100.0

        # STEP 4: PACKAGE AND PREPARE EXCLUSIVELY NUMERIC ROUTING DATA
        output_payload = np.column_stack((
            anomalies[:, 0],   # Ticker ID Reference [Column 0]
            anomalies[:, 2],   # Target Share Float [Column 1]
            final_calculated_demand,  # [Column 2]
            float_impact_percentage   # [Column 3]
        ))
        
        sort_indices = np.argsort(output_payload[:, 2])[::-1]
        return output_payload[sort_indices]


# =====================================================================
# REAL-TIME HIGH-SPEED NETWORKING LAYER (INLINE ARCHITECTURE)
# =====================================================================
class PhantomStreamConsumer:
    def __init__(self, engine: PhantomSqueezeEngine, api_token: str, stream_url: str, outbound_queue: asyncio.Queue):
        self.engine = engine
        self.api_token = api_token
        self.stream_url = stream_url
        self.outbound_queue = outbound_queue
        self.keep_running = True

    async def connect_and_stream_forever(self):
        """Maintains ultra-low latency linear execution with immediate hardware processing."""
        
        # Cache global and method references outside hot loops to achieve peak throughput
        push_payload = self.outbound_queue.put_nowait
        registry = self.engine.ticker_registry
        pipeline = self.engine.run_vectorized_pipeline
        
        while self.keep_running:
            try:
                async with websockets.connect(
                    self.stream_url, 
                    ping_interval=10, 
                    ping_timeout=5,
                    compression=None,
                    max_size=2**24
                ) as websocket:
                    
                    # 1. TRANSMIT AUTHENTICATION
                    await websocket.send(json.dumps({"action": "auth", "params": self.api_token}))
                    
                    # 2. SUBSCRIBE TO FULL MARKET CHANNELS
                    await websocket.send(json.dumps({"action": "subscribe", "params": "A.*,AM.*"}))
                    
                    # 3. HIGH-SPEED ZERO-LOG RECV LOOP
                    while self.keep_running:
                        raw_message = await websocket.recv()
                        data_chunk = json.loads(raw_message)
                        
                        if isinstance(data_chunk, dict):
                            raw_inputs = [data_chunk]
                        elif isinstance(data_chunk, list):
                            raw_inputs = data_chunk
                        else:
                            continue
                        
                        # Execute math inline on the same core frame
                        vector_results = pipeline(raw_inputs)
                        
                        if vector_results is not None:
                            for row in vector_results:
                                ticker_str = registry[int(row[0])]
                                share_float = row[1]
                                
                                execution_payload = {
                                    "ticker": ticker_str,
                                    "tier": 1 if share_float < 1_000_000 else 0,
                                    "shares": int(row[2]),
                                    "impact_pct": float(row[3])
                                }
                                # Push straight to outbound pipeline with zero execution lag
                                push_payload(execution_payload)
                                
            except (websockets.exceptions.ConnectionClosed, OSError):
                # Sub-second socket reset specifically for network flickering
                await asyncio.sleep(0.1)
                
            except Exception as e:
                # Retains deep debugging capabilities for computational or formatting bugs
                import traceback
                print(f"\n⚠️ SYSTEM ERROR DETECTED ON HOT PATH: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                await asyncio.sleep(1.0)


# =====================================================================
# HUMAN READABLE MONITORING SUB-SYSTEM (SPLITTER / DUMPER / READER)
# =====================================================================
async def anomaly_splitter_pipeline(outbound_queue: asyncio.Queue):
    """
    Acts as the main splitter gate. Pulls from the engine queue 
    and handles downstream dumping and viewing simultaneously.
    """
    # Open the raw file log safely outside the loop frame
    with open("anomalies_raw.log", "a", encoding="utf-8") as dumper_file:
        while True:
            # Pull the raw execution payload thrown by the consumer
            payload = await outbound_queue.get()
            
            # 1. THE DUMPER: Append raw data instantly to a historical file
            dumper_file.write(json.dumps(payload) + "\n")
            dumper_file.flush() 
            
            # 2. THE READER: Map numeric fields to a beautiful human-readable format
            ticker = payload["ticker"]
            shares = payload["shares"]
            impact_pct = payload["impact_pct"]
            tier_label = "💥 STRATOSPHERE (Ultra-Low Float)" if payload["tier"] == 1 else "⚡ STANDARD"
            timestamp = time.strftime('%H:%M:%S')

            # Clean dashboard display utilizing visual anchors for fast scannability
            print(f"\n[{timestamp}] 🚨 LIQUIDITY TRAP TRIGGERED: ${ticker}")
            print(f" └── Environment Tier : {tier_label}")
            print(f" └── Forced MM Demand : {shares:,.0f} Shares")
            print(f" └── Float Impact     : {impact_pct:.2f}% of Share Float")
            print("-" * 50)
            
            # Signal queue task completion
            outbound_queue.task_done()


# =====================================================================
# SYSTEM INITIALIZATION AND CONCURRENT TASK MANAGEMENT
# =====================================================================

async def main():
    loop = asyncio.get_running_loop()
    
    engine = PhantomSqueezeEngine()
    outbound_trade_queue = asyncio.Queue()
    
    TRADIER_API_TOKEN = "YOUR_TRADIER_PROD_OR_SANDBOX_TOKEN"
    TRADIER_STREAM_URL = "wss://://tradier.com"
    
    consumer = PhantomStreamConsumer(
        engine=engine,
        api_token=TRADIER_API_TOKEN,
        stream_url=TRADIER_STREAM_URL,
        outbound_queue=outbound_trade_queue
    )

    await asyncio.gather(
        consumer.run_stream_loop(),
        consumer.process_evaluation_pipeline()
    )

if __name__ == "__main__":
    asyncio.run(main())    
    
