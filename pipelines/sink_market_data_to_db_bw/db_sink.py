import logging
from dataclasses import dataclass
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from sqlalchemy import text
from pipelines.resources.postgres import get_db
ohlcv_table_creation = """
CREATE TABLE IF NOT EXISTS ohlcv (
    time timestamp NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    PRIMARY KEY (symbol, time)
);

-- 📊 Basic indexes cho queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_time ON ohlcv (time DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol ON ohlcv (symbol);
"""
stock_info_table_creation = """
CREATE TABLE IF NOT EXISTS stock_info (
    time timestamp NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    avg NUMERIC,
    ceil NUMERIC,
    floor NUMERIC,
    prior NUMERIC,
    data_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_stock_info_time ON stock_info (time DESC);
CREATE INDEX IF NOT EXISTS idx_stock_info_symbol ON stock_info (symbol);
"""

stock_tick_table_creation = """
CREATE TABLE IF NOT EXISTS stock_tick (
    time timestamp NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC,
    vol NUMERIC,
    side TEXT,
    data_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_stock_tick_time ON stock_tick (time DESC);
CREATE INDEX IF NOT EXISTS idx_stock_tick_symbol ON stock_tick (symbol);
"""

stock_top_price_table_creation = """
CREATE TABLE IF NOT EXISTS stock_top_price (
    time timestamp NOT NULL,
    symbol TEXT NOT NULL,
    bp JSONB,
    bq JSONB,
    ap JSONB,
    aq JSONB,
    total_bid NUMERIC,
    total_ask NUMERIC,
    data_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_stock_top_price_time ON stock_top_price (time DESC);
CREATE INDEX IF NOT EXISTS idx_stock_top_price_symbol ON stock_top_price (symbol);
"""

price_board_table_creation = """
CREATE TABLE IF NOT EXISTS price_board (
    time timestamp NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC,
    vol NUMERIC,
    total_vol NUMERIC,
    total_val NUMERIC,
    change NUMERIC,
    change_pct NUMERIC,
    data_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_price_board_time ON price_board (time DESC);
CREATE INDEX IF NOT EXISTS idx_price_board_symbol ON price_board (symbol);
"""

# Write-only insert queries (no ON CONFLICT)
ohlcv_insert_query = """
INSERT INTO ohlcv (time, symbol, open, high, low, close, volume) 
VALUES (to_timestamp(:time), :symbol, :open, :high, :low, :close, :volume)
"""

stock_info_insert_query = """
INSERT INTO stock_info (time, symbol, open, high, low, close, volume, avg, ceil, floor, prior, data_type) 
VALUES (to_timestamp(:time), :symbol, :open, :high, :low, :close, :volume, :avg, :ceil, :floor, :prior, :data_type)
"""

stock_tick_insert_query = """
INSERT INTO stock_tick (time, symbol, price, vol, side, data_type) 
VALUES (to_timestamp(:time), :symbol, :price, :vol, :side, :data_type)
"""

stock_top_price_insert_query = """
INSERT INTO stock_top_price (time, symbol, bp, bq, ap, aq, total_bid, total_ask, data_type) 
VALUES (to_timestamp(:time), :symbol, :bp, :bq, :ap, :aq, :total_bid, :total_ask, :data_type)
"""

price_board_insert_query = """
INSERT INTO price_board (time, symbol, price, vol, total_vol, total_val, change, change_pct, data_type) 
VALUES (to_timestamp(:time), :symbol, :price, :vol, :total_vol, :total_val, :change, :change_pct, :data_type)
"""

@dataclass
class DataQueryFields:
    query: str
    fields: list

_data_type_query_map = {
    "OH": DataQueryFields(
        query=ohlcv_insert_query,
        fields=["time", "symbol", "open", "high", "low", "close", "volume"]
    ),
    "OHLCV": DataQueryFields(
        query=ohlcv_insert_query,
        fields=["time", "symbol", "open", "high", "low", "close", "volume"]
    ),
    "SI": DataQueryFields(
        query=stock_info_insert_query,
        fields=["time", "symbol", "open", "high", "low", "close", "volume", "avg", "ceil", "floor", "prior", "data_type"]
    ),
    "ST": DataQueryFields(
        query=stock_tick_insert_query,
        fields=["time", "symbol", "price", "vol", "side", "data_type"]
    ),
    "TP": DataQueryFields(
        query=stock_top_price_insert_query,
        fields=["time", "symbol", "bp", "bq", "ap", "aq", "total_bid", "total_ask", "data_type"]
    ),
    "PB": DataQueryFields(
        query=price_board_insert_query,
        fields=["time", "symbol", "price", "vol", "total_vol", "total_val", "change", "change_pct", "data_type"]
    )
}

class _OHLCVSinkPartition(StatelessSinkPartition):
    def __init__(self):
        self.db = next(get_db())
        self._ensure_tables_exist()
        
    def _ensure_tables_exist(self):
        try:
            self.db.execute(text(ohlcv_table_creation))
            self.db.execute(text(stock_info_table_creation))
            self.db.execute(text(stock_tick_table_creation))
            self.db.execute(text(stock_top_price_table_creation))
            self.db.execute(text(price_board_table_creation))
            self.db.commit()
            logging.info("✅ All DNSE tables ready")
        except Exception as e:
            logging.warning(f"⚠️ Table creation warning: {e}")

    def write_batch(self, items):
        if not items:
            return
        
        success_count = 0
        error_count = 0
        
        for item in items:
            try:
                data_type = item.get("data_type", "OHLCV")
                query_fields = _data_type_query_map.get(data_type)
                
                if not query_fields:
                    logging.warning(f"⚠️ Unknown data type: {data_type}")
                    continue
                filtered_item = {k: item.get(k) for k in query_fields.fields if k in item}
                if data_type == "TP":
                    import json
                    if "bp" in filtered_item and isinstance(filtered_item["bp"], list):
                        filtered_item["bp"] = json.dumps(filtered_item["bp"])
                    if "bq" in filtered_item and isinstance(filtered_item["bq"], list):
                        filtered_item["bq"] = json.dumps(filtered_item["bq"])
                    if "ap" in filtered_item and isinstance(filtered_item["ap"], list):
                        filtered_item["ap"] = json.dumps(filtered_item["ap"])
                    if "aq" in filtered_item and isinstance(filtered_item["aq"], list):
                        filtered_item["aq"] = json.dumps(filtered_item["aq"])
                try:
                    self.db.execute(text(query_fields.query), filtered_item)
                    self.db.commit()
                    success_count += 1
                    
                except Exception as record_error:
                    error_count += 1
                    self.db.rollback()
                    logging.warning(f"⚠️ Skipped {data_type} record for {item.get('symbol', 'N/A')}: {str(record_error)[:100]}")
                
            except Exception as e:
                error_count += 1
                logging.error(f"❌ Error processing {item.get('data_type', 'UNKNOWN')}: {e}")
        
        if success_count > 0:
            logging.info(f"📤 Successfully inserted {success_count} DNSE records")
        if error_count > 0:
            logging.info(f"⚠️ Skipped {error_count} problematic records")

    def close(self):
        """Cleanup"""
        try:
            self.db.close()
        except Exception as e:
            logging.error(f"❌ Error closing: {e}")

class OHLCVSink(DynamicSink):
    """DNSE PostgreSQL Sink - Write Only Mode"""
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _OHLCVSinkPartition:
        return _OHLCVSinkPartition()

class _MarketDataSinkPartition(StatelessSinkPartition):
    def __init__(self):
        self.db = next(get_db())

    def write_batch(self, items):
        if not items:
            return
        for item in items:
            data_type = item.get("data_type", "OHLCV")
            query_fields = _data_type_query_map.get(data_type)
            if query_fields:
                filtered_item = {k: item.get(k) for k in query_fields.fields if k in item}
                self.db.execute(text(query_fields.query), filtered_item)
        self.db.commit()

    def close(self):
        try:
            self.db.close()
        except Exception as e:
            logging.error(f"Error closing: {e}")

class MarketDataSink(DynamicSink):
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _MarketDataSinkPartition:
        return _MarketDataSinkPartition()
