# ============================================
# gap_trading_v7.4_pd_lt_fillgap_rollback.py
# ============================================
# v7.3 기반 + (A) fill-gap 검증/즉시 롤백 + (B) BTC/ETH vs ALT entry gap 분리
# ============================================

import os
import sys
import time
import asyncio
import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Dict, Optional, Tuple

import aiohttp
import ccxt
from ccxt.base.errors import OperationRejected, AuthenticationError
from dotenv import load_dotenv

import lighter
from lighter.signer_client import SignerClient
from lighter.api.order_api import OrderApi
from lighter.api.account_api import AccountApi


class ParadexTokenExpired(Exception):
    pass


# ============================
# 로그 파일 설정
# ============================
log_filename = f"trading_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_v7_4.txt"
log_file = open(log_filename, "w", encoding="utf-8")

_original_print = print

def print(*args, **kwargs):
    _original_print(*args, **kwargs)
    try:
        if log_file and not log_file.closed:
            _original_print(*args, **kwargs, file=log_file, flush=True)
    except Exception:
        pass

print(f"📝 로그 파일: {log_filename}")
print("=" * 60)

# ============================
# Windows용 이벤트 루프 설정
# ============================
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ============================
# 설정
# ============================

load_dotenv()

# --- 포지션/리스크 ---
TARGET_PROFIT_USD = 0.25
POSITION_COLLATERAL_USD = 80.0
LEVERAGE = 10.0
POLL_INTERVAL = 0.1

# --- (중요) BTC/ETH vs ALT 진입 갭 분리 ---
MAJOR_COINS = {"BTC"}

# ✅ 너가 원하는 "전용 entry gap" 설정
MAJOR_ENTRY_GAP_PERCENT = 0.0005  # 예: 0.20%
ALT_ENTRY_GAP_PERCENT   = 0.001  # 예: 0.30%

def required_entry_gap_percent(coin: str) -> float:
    return MAJOR_ENTRY_GAP_PERCENT if coin in MAJOR_COINS else ALT_ENTRY_GAP_PERCENT

# --- event-driven ---
FRESH_MAX_AGE_SEC = 0.15
# --- Paradex aggressive ---
AGGRESSIVE_SLIPPAGE = 0.001  # 0.1%

# --- allow_stale (PnL/청산에서만) ---
ALLOW_STALE_FOR_PNL = True
ALLOW_STALE_FOR_CLOSE = True

# --- 수량 스텝(필요 시 코인별로 분리 가능) ---
SIZE_STEP = "0.001"

# --- (중요) fill-gap 검증 & 즉시 롤백 ---
# 진입 신호는 스냅샷으로 잡더라도, 실제 체결가 기준 갭(fill-gap)이 너무 작으면 즉시 롤백
MAJOR_MIN_FILL_GAP_PERCENT = 0.0005  # 예: 0.08%
ALT_MIN_FILL_GAP_PERCENT   = 0.001  # 예: 0.12%


# --- 롤백(=fill-gap 검증 후 즉시 청산) 사용 여부 ---
# 사용자가 롤백 없는 버전을 원할 때 False로 둔다.
ENABLE_FILLGAP_ROLLBACK = False

def min_fill_gap_percent(coin: str) -> float:
    return MAJOR_MIN_FILL_GAP_PERCENT if coin in MAJOR_COINS else ALT_MIN_FILL_GAP_PERCENT

# fill-gap 체크를 entry 후 몇 초 내에 할지(너무 늦으면 의미 없음)
FILL_GAP_CHECK_TIMEOUT_SEC = 5.0

# ============================
# 심볼 매핑
# ============================
COIN_MAPPING: Dict[str, str] = {
    "BTC": "BTC-USD-PERP",
    "ETH": "ETH-USD-PERP",
    "SOL": "SOL-USD-PERP",

    "HYPE": "HYPE-USD-PERP",

}

# ============================
# 환경 변수
# ============================
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY", "")
LIGHTER_ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "0"))
LIGHTER_API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "2"))

LIGHTER_URL = os.getenv("LIGHTER_URL", "https://mainnet.zklighter.elliot.ai")
LIGHTER_WS_URL = os.getenv("LIGHTER_WS_URL", "wss://mainnet.zklighter.elliot.ai/stream")

ETH_ADDRESS = os.getenv("ETHEREUM_ADDRESS")
ETH_PRIVATE_KEY = os.getenv("ETHEREUM_PRIVATE_KEY")
PARA_WS_URL = os.getenv("PARA_WS_URL", "wss://ws.api.prod.paradex.trade/v1")


# ============================
# 유틸
# ============================
def to_decimal(x):
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError):
        return None

def floor_quantity(notional_usd: float, price: float, step: str = SIZE_STEP) -> float:
    if price <= 0:
        return 0.0
    q = Decimal(str(notional_usd)) / Decimal(str(price))
    step_dec = Decimal(step)
    q = (q / step_dec).to_integral_value(rounding=ROUND_DOWN) * step_dec
    return float(q)

# ============================
# Notifier (event coalesce)
# ============================
class UpdateNotifier:
    def __init__(self):
        self._q: asyncio.Queue = asyncio.Queue(maxsize=1)

    def poke(self, source: str, key: str):
        payload = (time.time(), source, key)
        try:
            if self._q.full():
                try:
                    self._q.get_nowait()
                except Exception:
                    pass
            self._q.put_nowait(payload)
        except Exception:
            pass

    async def wait(self, timeout: float = 1.0):
        try:
            return await asyncio.wait_for(self._q.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None


# ============================
# Lighter market info
# ============================
async def initialize_market_info() -> Dict[str, dict]:
    market_info: Dict[str, dict] = {}
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{LIGHTER_URL}/api/v1/orderBooks") as resp:
            data = await resp.json()
            for m in data["order_books"]:
                market_info[m["symbol"].upper()] = {
                    "market_id": m["market_id"],
                    "size_decimals": m["supported_size_decimals"],
                    "price_decimals": m["supported_price_decimals"],
                }
    return market_info

async def create_lighter_market_order(
    client: SignerClient,
    market_info: dict,
    symbol: str,
    side: str,
    amount: float,
    best_bid: float = None,
    best_ask: float = None,
):
    m_info = market_info[symbol]
    market_index = m_info["market_id"]
    size_decimals = m_info["size_decimals"]
    price_decimals = m_info["price_decimals"]

    is_ask = 0 if side.lower() == "buy" else 1
    order_type_code = SignerClient.ORDER_TYPE_MARKET
    time_in_force = SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    client_order_index = 0
    order_expiry = 0

    amount_raw = int(round(float(amount) * (10**size_decimals)))

    if side.lower() == "buy":
        price = 2**63 - 1
    else:
        # ✅ 기존: price = 10**price_decimals  (BTC면 10 같은 말도 안되는 값 가능)
        # ✅ 개선: WS best_bid 기반으로 "조금 아래" 가격을 넣어서 IOC로 시장가처럼 체결 유도
        if best_bid is None:
            # 그래도 없으면 최소한 10 같은 값은 피하려고 1로 (마켓 정책에 따라 또 거절될 수 있음)
            price = 1
        else:
            # 0.2% 아래로 던져서 즉시 체결(너무 멀면 거절될 수 있어 과격하게 내리지 말 것)
            px = best_bid * (1 - 0.002)
            price = int(round(px * (10 ** price_decimals)))

    print(f"\n📤 [Lighter {symbol}] 주문 전송")
    print(f"   Market ID : {market_index}")
    print(f"   Side      : {side.upper()}")
    print(f"   Amount    : {amount_raw} (raw, {amount} {symbol})")
    print(f"   Price     : {price} (시장가 흉내)")

    resp = await client.create_order(
        market_index=market_index,
        client_order_index=client_order_index,
        base_amount=amount_raw,
        price=price,
        is_ask=is_ask,
        order_type=order_type_code,
        time_in_force=time_in_force,
        order_expiry=order_expiry,
    )
    r = resp[1] if isinstance(resp, (list, tuple)) and len(resp) > 1 else None
    if r is None:
        print(f"   ❌ [Lighter {symbol}] create_order 응답 None. raw_resp={resp}")
        return {"code": "NO_RESPONSE", "message": "create_order returned None", "tx_hash": None}

    return {"code": r.code, "message": r.message, "tx_hash": r.tx_hash}


class LighterWSFeed:
    def __init__(self, url: str, market_id: int, notifier: Optional[UpdateNotifier] = None, symbol: str = ""):
        self._url = url
        self._market_id = market_id
        self._symbol = symbol
        self._notifier = notifier

        self._bid: Optional[float] = None
        self._ask: Optional[float] = None
        self._bid_ts = 0.0
        self._ask_ts = 0.0

        self._ready = asyncio.Event()
        self._stop = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[Lighter WS] stop 예외: {e}")

    async def wait_until_ready(self, timeout: float = 10.0):
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    def get_last_update_ts(self) -> float:
        return self._bid_ts if self._bid_ts > self._ask_ts else self._ask_ts

    def get_prices(self, max_age_sec: float = 0.5, max_spread_usd: float = 999999.0, allow_stale: bool = False):
        bid = self._bid
        ask = self._ask
        if bid is None or ask is None:
            return None, None, None

        now = time.time()
        too_old = (now - self._bid_ts > max_age_sec) or (now - self._ask_ts > max_age_sec)
        if too_old and not allow_stale:
            return None, None, None

        spread = ask - bid
        if spread <= 0 or spread > max_spread_usd:
            return None, None, None

        mid = (bid + ask) / 2.0
        return bid, ask, mid

    async def _run(self):
        channel = f"order_book/{self._market_id}"
        print(f"🌐 [Lighter WS] 연결 시작: {self._symbol} channel={channel}")

        while not self._stop:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self._url, heartbeat=30) as ws:
                        print(f"[Lighter WS] connected: {self._symbol}")
                        await ws.send_json({"type": "subscribe", "channel": channel})

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = msg.json()
                                except Exception:
                                    continue

                                if data.get("type") != "update/order_book":
                                    continue

                                data_channel = data.get("channel", "")
                                if data_channel not in (channel, channel.replace("/", ":")):
                                    continue

                                ob = data.get("order_book") or {}
                                bids = ob.get("bids") or []
                                asks = ob.get("asks") or []

                                best_bid = None
                                best_ask = None
                                if bids:
                                    b0 = bids[0]
                                    best_bid = float(b0["price"]) if isinstance(b0, dict) else float(b0[0])
                                if asks:
                                    a0 = asks[0]
                                    best_ask = float(a0["price"]) if isinstance(a0, dict) else float(a0[0])

                                if best_bid is None and best_ask is None:
                                    continue

                                now_ts = time.time()
                                if best_bid is not None:
                                    self._bid = best_bid
                                    self._bid_ts = now_ts
                                if best_ask is not None:
                                    self._ask = best_ask
                                    self._ask_ts = now_ts

                                if self._bid is not None and self._ask is not None and not self._ready.is_set():
                                    self._ready.set()

                                if self._notifier is not None:
                                    self._notifier.poke("lighter", self._symbol)

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

            except Exception as e:
                print(f"[Lighter WS] error({self._symbol}): {e}")

            if not self._stop:
                await asyncio.sleep(3.0)




# ============================
# LAST TRADE 기반 가격 피드 (Synthetic bid/ask)
# - "order_book" 대신 "trade" 스트림을 구독해서
#   최근 체결가로 bid/ask를 근사합니다.
#   * BUY 체결(공격적 매수) -> ask 로 간주
#   * SELL 체결(공격적 매도) -> bid 로 간주
# ============================

class LighterLastTradeFeed:
    """
    Lighter WS Trade Channel 기반.
    - 구독: trade/{MARKET_INDEX}
    - 메시지: type=update/trade, channel=trade:{MARKET_INDEX}, trades=[...]
    Lighter trade payload에 is_maker_ask 가 있어 공격자(side)를 추정할 수 있음:
      - is_maker_ask == True  -> maker가 ask(매도 지정가), taker=BUY  => BUY 체결(ask)
      - is_maker_ask == False -> maker가 bid(매수 지정가), taker=SELL => SELL 체결(bid)
    Docs: https://apidocs.lighter.xyz/docs/websocket-reference
    """
    def __init__(self, url: str, market_id: int, notifier: Optional[UpdateNotifier] = None, symbol: str = ""):
        self._url = url
        self._market_id = market_id
        self._symbol = symbol
        self._notifier = notifier

        self._bid: Optional[float] = None   # last SELL trade price
        self._ask: Optional[float] = None   # last BUY trade price
        self._last: Optional[float] = None  # last trade price (any)
        self._ts = 0.0

        # [KEEP LAST VALID] reconnect/quiet 구간에서도 마지막 유효 값을 유지
        self._bid_valid: Optional[float] = None
        self._ask_valid: Optional[float] = None
        self._last_valid: Optional[float] = None
        self._ts_valid = 0.0

        self._ready = asyncio.Event()
        self._stop = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[Lighter TRADE] stop 예외: {e}")

    async def wait_until_ready(self, timeout: float = 10.0):
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    def get_last_update_ts(self) -> float:
        return self._ts

    def get_prices(self, max_age_sec: float = 0.5, max_spread_usd: float = 999999.0, allow_stale: bool = False):
        now = time.time()
        if (now - self._ts > max_age_sec) and not allow_stale:
            return None, None, None

        bid = self._bid
        ask = self._ask
        last = self._last

        # 한쪽이 비어있으면 last로 보정(스프레드=0 허용)
        if last is None:
            return None, None, None
        if bid is None:
            bid = last
        if ask is None:
            ask = last

        spread = ask - bid
        if spread < 0 or spread > max_spread_usd:
            return None, None, None

        mid = (bid + ask) / 2.0
        return bid, ask, mid

    async def _run(self):
        channel = f"trade/{self._market_id}"
        expected_channel_1 = f"trade:{self._market_id}"
        expected_channel_2 = f"trade/{self._market_id}"

        print(f"🌐 [Lighter TRADE] 연결 시작: {self._symbol} channel={channel}")

        while not self._stop:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self._url, heartbeat=30) as ws:
                        print(f"[Lighter TRADE] connected: {self._symbol}")
                        await ws.send_json({"type": "subscribe", "channel": channel})

                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue

                            try:
                                data = msg.json()
                            except Exception:
                                continue

                            if data.get("type") != "update/trade":
                                continue

                            ch = data.get("channel", "")
                            if ch not in (expected_channel_1, expected_channel_2):
                                continue

                            trades = data.get("trades") or []
                            if not trades:
                                continue

                            # trades는 배열로 오며 가장 마지막이 최신이라고 가정(방어적으로 reverse iterate)
                            updated = False
                            for t in reversed(trades):
                                try:
                                    price = float(t.get("price"))
                                except Exception:
                                    continue

                                self._last = price
                                # 공격자 side 추정
                                is_maker_ask = t.get("is_maker_ask")
                                if is_maker_ask is True:
                                    # taker BUY => ask
                                    self._ask = price
                                elif is_maker_ask is False:
                                    # taker SELL => bid
                                    self._bid = price
                                else:
                                    # 정보 없으면 last만 업데이트
                                    pass

                                updated = True
                                break

                            if not updated:
                                continue

                            now_ts = time.time()
                            self._ts = now_ts
                            # keep last valid (even if later feed goes quiet)
                            self._last_valid = price
                            # Lighter trade payload uses is_maker_ask to infer taker side
                            # is_maker_ask=True  -> taker BUY (hit ask)  -> update ask_valid
                            # is_maker_ask=False -> taker SELL (hit bid) -> update bid_valid
                            if is_maker_ask is True:
                                self._ask_valid = price
                            elif is_maker_ask is False:
                                self._bid_valid = price
                            self._ts_valid = now_ts

                            if not self._ready.is_set():
                                self._ready.set()

                            if self._notifier is not None:
                                self._notifier.poke("lighter_trade", self._symbol)

            except Exception as e:
                print(f"[Lighter TRADE] error({self._symbol}): {e}")

            if not self._stop:
                await asyncio.sleep(3.0)


class ParadexLastTradeFeed:
    """
    Paradex Public WS Trades Channel 기반.
    - 구독: trades.{MARKET} (BBO 채널 네이밍과 동일한 패턴 가정)
    - 메시지: method=subscription, params.data: {price, side(BUY/SELL), ...}
    Docs: https://docs.paradex.trade/ws/web-socket-channels/trades/trades
    """
    def __init__(self, url: str, market_symbol: str, notifier: Optional[UpdateNotifier] = None, coin: str = ""):
        self._url = url
        self._market_symbol = market_symbol
        self._channel = f"trades.{market_symbol}"
        self._coin = coin
        self._notifier = notifier

        self._bid: Optional[float] = None   # last SELL trade price
        self._ask: Optional[float] = None   # last BUY trade price
        self._last: Optional[float] = None  # last trade price
        self._ts = 0.0

        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._ready = asyncio.Event()

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[Paradex TRADES] stop 예외: {e}")

    async def wait_until_ready(self, timeout: float = 10.0):
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    def get_last_update_ts(self) -> float:
        return self._ts

    def get_prices(self, max_age_sec: float = 0.5, max_spread_usd: float = 999999.0, allow_stale: bool = False):
        now = time.time()
        if (now - self._ts > max_age_sec) and not allow_stale:
            return None, None, None

        bid = self._bid
        ask = self._ask
        last = self._last
        if last is None:
            # If feed got quiet or reconnected, keep using last valid when allowed
            if allow_stale and self._last_valid is not None:
                last = self._last_valid
                bid = self._bid_valid
                ask = self._ask_valid
            else:
                return None, None, None

        if bid is None:
            bid = last
        if ask is None:
            ask = last

        spread = ask - bid
        if spread < 0 or spread > max_spread_usd:
            return None, None, None

        mid = (bid + ask) / 2.0
        return bid, ask, mid

    async def _run(self):
        print(f"🌐 [Paradex TRADES] 연결 시작: {self._coin} channel={self._channel}")

        while not self._stop:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self._url, heartbeat=30) as ws:
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "method": "subscribe",
                            "params": {"channel": self._channel},
                            "id": 1,
                        }
                        await ws.send_json(sub_msg)
                        print(f"[Paradex TRADES] connected: {self._coin}")

                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue

                            try:
                                data = msg.json()
                            except Exception:
                                continue

                            if data.get("method") != "subscription":
                                continue

                            params = data.get("params") or {}
                            if params.get("channel") != self._channel:
                                continue

                            d = params.get("data") or {}
                            price_str = d.get("price")
                            side = (d.get("side") or "").upper()
                            if price_str is None:
                                continue

                            try:
                                price = float(price_str)
                            except Exception:
                                continue

                            self._last = price
                            if side == "BUY":
                                self._ask = price
                            elif side == "SELL":
                                self._bid = price
                            else:
                                # side 없으면 last만 업데이트
                                pass

                            self._ts = time.time()

                            if not self._ready.is_set():
                                self._ready.set()

                            if self._notifier is not None:
                                self._notifier.poke("paradex_trade", self._coin or self._market_symbol)

            except Exception as e:
                print(f"[Paradex TRADES] error({self._coin}): {e}")

            if not self._stop:
                await asyncio.sleep(3.0)


# ============================
# Paradex
# ============================
def create_paradex_client() -> ccxt.Exchange:
    if not ETH_ADDRESS or not ETH_PRIVATE_KEY:
        raise ValueError("ETHEREUM_ADDRESS / ETHEREUM_PRIVATE_KEY 환경 변수를 확인하세요.")

    exchange = ccxt.paradex({
        "walletAddress": ETH_ADDRESS,
        "privateKey": ETH_PRIVATE_KEY,
        "enableRateLimit": True,
    })
    exchange.load_markets()
    return exchange

class ParadexBBOFeed:
    def __init__(self, url: str, market_symbol: str, notifier: Optional[UpdateNotifier] = None, coin: str = ""):
        self._url = url
        self._market_symbol = market_symbol
        self._channel = f"bbo.{market_symbol}"
        self._coin = coin
        self._notifier = notifier

        self._bid: Optional[float] = None
        self._ask: Optional[float] = None
        self._last_update_ts = 0.0

        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._ready = asyncio.Event()

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[Paradex WS] stop 예외: {e}")

    async def wait_until_ready(self, timeout: float = 10.0):
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    def get_last_update_ts(self) -> float:
        return self._last_update_ts

    def get_prices(self, max_age_sec: float = 0.5, max_spread_usd: float = 999999.0, allow_stale: bool = False):
        bid = self._bid
        ask = self._ask
        if bid is None or ask is None:
            return None, None, None

        now = time.time()
        if (now - self._last_update_ts > max_age_sec) and not allow_stale:
            return None, None, None

        spread = ask - bid
        if spread <= 0 or spread > max_spread_usd:
            return None, None, None

        mid = (bid + ask) / 2.0
        return bid, ask, mid

    async def _run(self):
        print(f"🌐 [Paradex WS] 연결 시작: {self._coin} channel={self._channel}")

        while not self._stop:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self._url, heartbeat=30) as ws:
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "method": "subscribe",
                            "params": {"channel": self._channel},
                            "id": 1,
                        }
                        await ws.send_json(sub_msg)
                        print(f"[Paradex WS] connected: {self._coin}")

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = msg.json()
                                except Exception:
                                    continue

                                if data.get("method") != "subscription":
                                    continue

                                params = data.get("params") or {}
                                if params.get("channel") != self._channel:
                                    continue

                                d = params.get("data") or {}
                                bid_str = d.get("bid")
                                ask_str = d.get("ask")
                                if bid_str is None or ask_str is None:
                                    continue

                                try:
                                    bid = float(bid_str)
                                    ask = float(ask_str)
                                except Exception:
                                    continue

                                self._bid = bid
                                self._ask = ask
                                self._last_update_ts = time.time()

                                if not self._ready.is_set():
                                    self._ready.set()

                                if self._notifier is not None:
                                    self._notifier.poke("paradex", self._coin or self._market_symbol)

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

            except Exception as e:
                print(f"[Paradex WS] error({self._coin}): {e}")

            if not self._stop:
                await asyncio.sleep(3.0)


def calc_aggressive_limit_price(
    exchange: ccxt.Exchange,
    symbol: str,
    side: str,
    slippage: float,
    best_bid: Optional[float] = None,
    best_ask: Optional[float] = None,
) -> float:
    if best_bid is None or best_ask is None:
        ob = exchange.fetch_order_book(symbol)
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None

    if side.lower() == "buy":
        base = best_ask if best_ask is not None else best_bid
        if base is None:
            raise RuntimeError(f"[Paradex {symbol}] empty orderbook (buy)")
        raw_price = base * (1 + slippage)
    else:
        base = best_bid if best_bid is not None else best_ask
        if base is None:
            raise RuntimeError(f"[Paradex {symbol}] empty orderbook (sell)")
        raw_price = base * (1 - slippage)

    precise_price = float(exchange.price_to_precision(symbol, raw_price))
    print(
        f"   [Paradex Aggressive {symbol}] best_bid={best_bid}, best_ask={best_ask}, "
        f"calc_price={raw_price} → precise_price={precise_price}"
    )
    return precise_price


def place_paradex_aggressive_limit(
    exchange: ccxt.Exchange,
    symbol: str,
    side: str,
    amount: float,
    reduce_only: bool = False,
    best_bid: Optional[float] = None,
    best_ask: Optional[float] = None,
):
    price = calc_aggressive_limit_price(
        exchange, symbol, side, AGGRESSIVE_SLIPPAGE,
        best_bid=best_bid, best_ask=best_ask
    )

    params = {}
    if reduce_only:
        params["reduceOnly"] = True

    print(f"\n📤 [Paradex {symbol}] 지정가 주문 전송: {side.upper()} {amount} @ {price}, reduceOnly={reduce_only}")

    try:
        order = exchange.create_order(
            symbol=symbol,
            type="limit",
            side=side,
            amount=amount,
            price=price,
            params=params,
        )
        print(f"   ✅ Paradex 주문 응답: {order}")
        return order
    except OperationRejected as e:
        msg = str(e)
        print(f"   ⚠️ [Paradex] 주문 거절: {msg}")
        if "INVALID_TOKEN" in msg or "token is expired" in msg:
            raise ParadexTokenExpired(msg) from e
        raise
    except AuthenticationError as e:
        print(f"   ⚠️ [Paradex] 인증 오류: {e}")
        raise


# ============================
# Entry/size 추출 (Lighter)
# ============================
def extract_lighter_entry_from_account(account, market_id: int):
    accounts = getattr(account, "accounts", None)
    if not accounts:
        return None, None

    acc = accounts[0]
    positions = getattr(acc, "positions", None)
    if not positions:
        return None, None

    for pos in positions:
        pid = getattr(pos, "market_id", None)
        if pid != market_id:
            continue

        raw_pos = getattr(pos, "position", None)
        raw_entry = getattr(pos, "avg_entry_price", None)
        sign = getattr(pos, "sign", 1)

        if raw_pos is None or raw_entry is None:
            continue

        try:
            size_abs = float(raw_pos)
            entry_price = float(raw_entry)
            size_signed = size_abs * (1 if sign == 1 else -1)
            return entry_price, size_signed
        except Exception:
            continue

    return None, None


async def get_lighter_position_with_retry(account_api, market_id, coin, lighter_account_index, max_retries=3):
    delays = [1.0, 2.0, 3.0]
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                await asyncio.sleep(delays[attempt - 1])

            print(f"   🔍 [Lighter {coin}] account 기반 entry/size 조회... ({attempt+1}/{max_retries})")
            account = await account_api.account(by="index", value=str(lighter_account_index))
            entry, size = extract_lighter_entry_from_account(account, market_id)
            if entry is not None and size is not None and entry > 0 and abs(size) > 0:
                print(f"   ✅ [Lighter {coin}] ENTRY 세팅 성공: entry={entry:.6f}, size={size:.6f}")
                return entry, size, True
            else:
                print(f"   ⚠️ [Lighter {coin}] 유효 포지션 없음 (entry={entry}, size={size})")
        except Exception as e:
            print(f"   ⚠️ [Lighter {coin}] 조회 오류: {e}")

    print(f"   ❌ [Lighter {coin}] 재시도 실패 → fallback 유지")
    return None, None, False


# ============================
# Fill-gap 계산 (실제 체결가 기준)
# ============================
def calc_fill_gap(coin: str, direction: str, lt_entry: float, pd_entry: float) -> Tuple[float, float]:
    """
    direction:
      - LT_LONG_PD_SHORT : LT buy entry, PD sell entry  => gap_usd = pd_entry - lt_entry
      - LT_SHORT_PD_LONG : LT sell entry, PD buy entry  => gap_usd = lt_entry - pd_entry
    """
    if direction == "LT_LONG_PD_SHORT":
        gap_usd = pd_entry - lt_entry
        denom = lt_entry if lt_entry > 0 else 1.0
        gap_pct = gap_usd / denom
    else:
        gap_usd = lt_entry - pd_entry
        denom = lt_entry if lt_entry > 0 else 1.0
        gap_pct = gap_usd / denom

    return gap_usd, gap_pct


# ============================
# 메인 루프
# ============================
async def gap_trading_loop():
    if not API_KEY_PRIVATE_KEY or LIGHTER_ACCOUNT_INDEX == 0:
        print("❌ Lighter 환경 변수를 확인하세요 (API_KEY_PRIVATE_KEY, ACCOUNT_INDEX).")
        return

    client = SignerClient(
        url=LIGHTER_URL,
        private_key=API_KEY_PRIVATE_KEY,
        account_index=LIGHTER_ACCOUNT_INDEX,
        api_key_index=LIGHTER_API_KEY_INDEX,
    )
    OrderApi(client.api_client)  # keep for parity
    account_api = AccountApi(client.api_client)

    print("\n1️⃣ Lighter 마켓 정보 로딩...")
    market_info = await initialize_market_info()

    print("\n2️⃣ Paradex 클라이언트 생성...")
    pd = create_paradex_client()

    notifier = UpdateNotifier()

    lighter_feeds: Dict[str, LighterLastTradeFeed] = {}
    paradex_feeds: Dict[str, ParadexLastTradeFeed] = {}

    for coin, para_symbol in COIN_MAPPING.items():
        if coin not in market_info:
            print(f"⚠️ {coin} - Lighter에 없음, 스킵")
            continue

        market_id = market_info[coin]["market_id"]

        lt_feed = LighterLastTradeFeed(LIGHTER_WS_URL, market_id, notifier=notifier, symbol=coin)
        await lt_feed.start()
        try:
            await lt_feed.wait_until_ready(timeout=30.0)
        except Exception:
            print(f"⚠️ [Lighter {coin}] WS 준비 타임아웃(진행)")

        pd_feed = ParadexLastTradeFeed(PARA_WS_URL, para_symbol, notifier=notifier, coin=coin)
        await pd_feed.start()
        try:
            await pd_feed.wait_until_ready(timeout=30.0)
        except Exception:
            print(f"⚠️ [Paradex {coin}] WS 준비 타임아웃(진행)")

        lighter_feeds[coin] = lt_feed
        paradex_feeds[coin] = pd_feed

    coins = list(lighter_feeds.keys())
    print(f"\n✅ 모니터링 코인: {coins}")
    print("🚀 v13 (LAST TRADE 기반) 시작")
    print(f"   ENTRY GAP: majors(BTC/ETH)={MAJOR_ENTRY_GAP_PERCENT*100:.3f}% | alts={ALT_ENTRY_GAP_PERCENT*100:.3f}%")
    if ENABLE_FILLGAP_ROLLBACK:
        print(f"   MIN FILL GAP(rollback): majors={MAJOR_MIN_FILL_GAP_PERCENT*100:.3f}% | alts={ALT_MIN_FILL_GAP_PERCENT*100:.3f}%")
    else:
        print("   MIN FILL GAP(rollback): DISABLED")
    print("")

    # 상태
    in_position = False
    current_coin: Optional[str] = None
    direction: Optional[str] = None
    qty_coin = 0.0

    lighter_side: Optional[str] = None
    paradex_side: Optional[str] = None

    lighter_entry_price: Optional[float] = None
    lighter_pos_size: Optional[float] = None

    paradex_entry_price: Optional[float] = None
    paradex_pos_size: Optional[float] = None

    entry_gap_pct_snapshot: Optional[float] = None
    position_entry_time = 0.0
    exit_signal_count = 0
    last_pnl_log = 0.0

    lighter_entry_needs_refresh = False
    paradex_entry_needs_refresh = False

    # v7.4: fill-gap check state
    fillgap_checked = False
    fillgap_check_deadline = 0.0

    async def emergency_close_all(reason: str = ""):
        nonlocal in_position, current_coin, direction, qty_coin
        nonlocal lighter_side, paradex_side
        nonlocal lighter_entry_price, lighter_pos_size, paradex_entry_price, paradex_pos_size
        nonlocal entry_gap_pct_snapshot, position_entry_time, exit_signal_count
        nonlocal lighter_entry_needs_refresh, paradex_entry_needs_refresh
        nonlocal fillgap_checked, fillgap_check_deadline

        if not in_position or current_coin is None or direction is None:
            return

        print("\n" + "=" * 60)
        print("🚨 롤백/긴급 청산 실행")
        if reason:
            print(f"   사유: {reason}")
        print(f"   coin={current_coin}, dir={direction}, qty={qty_coin:.6f}")
        print("=" * 60)

        para_symbol = COIN_MAPPING[current_coin]
        lt_feed = lighter_feeds.get(current_coin)
        pd_feed = paradex_feeds.get(current_coin)

        if direction == "LT_SHORT_PD_LONG":
            lt_close_side = "buy"
            pd_close_side = "sell"
        else:
            lt_close_side = "sell"
            pd_close_side = "buy"

        lt_bid, lt_ask, _ = (None, None, None)
        pd_bid, pd_ask, _ = (None, None, None)
        if lt_feed:
            lt_bid, lt_ask, _ = lt_feed.get_prices(allow_stale=True)
        if pd_feed:
            pd_bid, pd_ask, _ = pd_feed.get_prices(allow_stale=True)

        try:
            lt_close_task = asyncio.create_task(
                create_lighter_market_order(client, market_info, current_coin, lt_close_side, qty_coin)
            )
            pd_close_task = asyncio.to_thread(
                place_paradex_aggressive_limit,
                pd, para_symbol, pd_close_side, qty_coin, True, pd_bid, pd_ask
            )

            lt_res, pd_res = await asyncio.gather(lt_close_task, pd_close_task, return_exceptions=True)

            if isinstance(lt_res, Exception):
                print(f"   ❌ [Lighter] 청산 실패: {lt_res}")
            else:
                print(f"   ✅ [Lighter] 청산 완료: {lt_res}")

            if isinstance(pd_res, Exception):
                print(f"   ❌ [Paradex] 청산 실패: {pd_res}")
            else:
                print(f"   ✅ [Paradex] 청산 완료")

        except Exception as e:
            print(f"❌ 청산 중 예외: {e}")

        # reset state
        in_position = False
        current_coin = None
        direction = None
        qty_coin = 0.0
        lighter_side = None
        paradex_side = None
        lighter_entry_price = None
        lighter_pos_size = None
        paradex_entry_price = None
        paradex_pos_size = None
        entry_gap_pct_snapshot = None
        position_entry_time = 0.0
        exit_signal_count = 0
        lighter_entry_needs_refresh = False
        paradex_entry_needs_refresh = False
        fillgap_checked = False
        fillgap_check_deadline = 0.0

    try:
        last_token_refresh = time.time()
        TOKEN_REFRESH_INTERVAL = 300

        while True:
            now = time.time()

            # Paradex 토큰 예방 갱신
            if now - last_token_refresh > TOKEN_REFRESH_INTERVAL:
                print(f"\n🔄 Paradex 토큰 갱신 ({TOKEN_REFRESH_INTERVAL//60}분 주기)")
                try:
                    pd = create_paradex_client()
                    last_token_refresh = now
                    print("   ✅ 토큰 갱신 완료")
                except Exception as e:
                    print(f"   ❌ 토큰 갱신 실패: {e}")

            # ============================================
            # 포지션 없음: event-driven 스캔 (NO CONFIRM)
            # ============================================
            if not in_position:
                await notifier.wait(timeout=1.0)
                now = time.time()

                best_candidate: Optional[Tuple[str, str]] = None
                best_gap_pct = 0.0
                best_gap_usd = 0.0
                best_snapshot = None

                for coin in coins:
                    lt_feed = lighter_feeds.get(coin)
                    pd_feed = paradex_feeds.get(coin)
                    if lt_feed is None or pd_feed is None:
                        continue

                    # freshness
                    lt_ts = lt_feed.get_last_update_ts()
                    pd_ts = pd_feed.get_last_update_ts()
                    if (now - lt_ts) > FRESH_MAX_AGE_SEC or (now - pd_ts) > FRESH_MAX_AGE_SEC:
                        continue

                    lt_bid, lt_ask, _ = lt_feed.get_prices(max_age_sec=FRESH_MAX_AGE_SEC, allow_stale=False)
                    pd_bid, pd_ask, _ = pd_feed.get_prices(max_age_sec=FRESH_MAX_AGE_SEC, allow_stale=False)
                    if lt_bid is None or lt_ask is None or pd_bid is None or pd_ask is None:
                        continue

                    # 방향별 체결가능 기준 갭
                    gap_short_usd = lt_bid - pd_ask          # LT sell@bid, PD buy@ask
                    gap_long_usd  = pd_bid - lt_ask          # LT buy@ask, PD sell@bid

                    gap_short_pct = (gap_short_usd / lt_bid) if lt_bid > 0 else 0.0
                    gap_long_pct  = (gap_long_usd / lt_ask) if lt_ask > 0 else 0.0

                    req_gap = required_entry_gap_percent(coin)

                    selected_direction = None
                    selected_gap_pct = 0.0
                    selected_gap_usd = 0.0

                    if gap_long_pct >= gap_short_pct and gap_long_pct >= req_gap:
                        selected_direction = "LT_LONG_PD_SHORT"
                        selected_gap_pct = gap_long_pct
                        selected_gap_usd = gap_long_usd
                    elif gap_short_pct >= req_gap:
                        selected_direction = "LT_SHORT_PD_LONG"
                        selected_gap_pct = gap_short_pct
                        selected_gap_usd = gap_short_usd
                    else:
                        continue

                    if selected_gap_pct > best_gap_pct:
                        best_gap_pct = selected_gap_pct
                        best_gap_usd = selected_gap_usd
                        best_candidate = (coin, selected_direction)
                        best_snapshot = (lt_bid, lt_ask, pd_bid, pd_ask, req_gap)

                if best_candidate is None:
                    continue

                # ✅ NO CONFIRM: 갭 조건 만족 즉시 진입
                coin, selected_direction = best_candidate
                if best_snapshot:
                    _, _, _, _, req_gap = best_snapshot
                else:
                    req_gap = required_entry_gap_percent(coin)

# 진입 확정
                if not best_snapshot:
                    continue

                lt_bid, lt_ask, pd_bid, pd_ask, req_gap = best_snapshot

                print("\n==============================")
                print(f"🚨 진입 신호 확정! (NO CONFIRM)")
                print(f"   coin={coin}, dir={selected_direction}, gap={best_gap_pct*100:.4f}% ({best_gap_usd:+.6f} USD)")
                print(f"   req_gap={req_gap*100:.4f}% (majors/alt split)")
                print("==============================")

                # reset confirm

                # 수량 산정
                position_value = POSITION_COLLATERAL_USD * LEVERAGE
                entry_price_est = lt_ask if selected_direction == "LT_LONG_PD_SHORT" else lt_bid
                qty_coin = floor_quantity(position_value, entry_price_est, step=SIZE_STEP)
                if qty_coin <= 0:
                    print("⚠️ 계산 수량 0 → 스킵")
                    continue

                # sides
                if selected_direction == "LT_SHORT_PD_LONG":
                    lighter_side = "sell"
                    paradex_side = "buy"
                else:
                    lighter_side = "buy"
                    paradex_side = "sell"

                current_coin = coin
                direction = selected_direction
                entry_gap_pct_snapshot = best_gap_pct

                # fallback entry
                lighter_entry_price = lt_ask if lighter_side == "buy" else lt_bid
                paradex_entry_price = pd_bid if paradex_side == "sell" else pd_ask

                print("\n🔍 [DEBUG] 진입 시점 가격 정보:")
                print(f"   current_coin={current_coin}")
                print(f"   direction={direction}")
                print(f"   lighter_entry_price(fallback)={lighter_entry_price:.6f}")
                print(f"   paradex_entry_price(fallback)={paradex_entry_price:.6f}")
                print(f"   Lighter bid/ask={lt_bid:.6f}/{lt_ask:.6f}")
                print(f"   Paradex bid/ask={pd_bid:.6f}/{pd_ask:.6f}")
                print("⚡ 양쪽 주문 전송 중...(동시 실행)")

                para_symbol = COIN_MAPPING[current_coin]

                lt_task = asyncio.create_task(
                    create_lighter_market_order(client, market_info, current_coin, lighter_side, qty_coin, best_bid=lt_bid, best_ask=lt_ask)
                )
                pd_task = asyncio.to_thread(
                    place_paradex_aggressive_limit,
                    pd, para_symbol, paradex_side, qty_coin, False, pd_bid, pd_ask
                )

                try:
                    lt_res, _ = await asyncio.gather(lt_task, pd_task)
                    print(f"   ✅ [Lighter {current_coin}] 주문 완료: {lt_res}")
                    print("🎉 포지션 진입 완료! → entry/fill-gap 검증 진행\n")
                except ParadexTokenExpired:
                    print("   ⚠️ Paradex 토큰 만료 → 클라이언트 재생성 후 재시도")
                    pd = create_paradex_client()
                    pd_task2 = asyncio.to_thread(
                        place_paradex_aggressive_limit,
                        pd, para_symbol, paradex_side, qty_coin, False, pd_bid, pd_ask
                    )
                    await pd_task2
                    lt_res = await lt_task
                    print(f"   ✅ [Lighter {current_coin}] 주문 완료(재시도 후): {lt_res}")
                except Exception as e:
                    print(f"❌ 진입 중 예외: {e}")
                    # 진입 실패면 상태 리셋
                    in_position = False
                    current_coin = None
                    direction = None
                    qty_coin = 0.0
                    lighter_side = None
                    paradex_side = None
                    lighter_entry_price = None
                    paradex_entry_price = None
                    entry_gap_pct_snapshot = None
                    continue

                in_position = True
                position_entry_time = time.time()
                exit_signal_count = 0

                lighter_entry_needs_refresh = True
                paradex_entry_needs_refresh = True

                fillgap_checked = False
                fillgap_check_deadline = position_entry_time + FILL_GAP_CHECK_TIMEOUT_SEC

                await asyncio.sleep(POLL_INTERVAL)
                continue

            # ============================================
            # 포지션 보유 중: entry/size 채우기
            # ============================================
            if in_position and current_coin is not None and direction is not None:
                para_symbol = COIN_MAPPING[current_coin]
                market_id = market_info[current_coin]["market_id"]

                # Lighter entry refresh
                if lighter_entry_needs_refresh:
                    entry, size, ok = await get_lighter_position_with_retry(
                        account_api, market_id, current_coin, LIGHTER_ACCOUNT_INDEX
                    )
                    if ok:
                        print("\n🔍 [DEBUG] Lighter 실제 체결 정보:")
                        print(f"   fallback entry: {lighter_entry_price:.6f}")
                        print(f"   actual entry:   {entry:.6f}")
                        print(f"   diff:           {(entry - (lighter_entry_price or 0.0)):+.6f}")
                        lighter_entry_price = entry
                        lighter_pos_size = size
                    lighter_entry_needs_refresh = False

                # Paradex entry refresh
                if paradex_entry_needs_refresh:
                    try:
                        print(f"   🔍 [Paradex {para_symbol}] fetch_position 기반 entry/size 조회...")
                        await asyncio.sleep(1.5)
                        pos = await asyncio.to_thread(pd.fetch_position, para_symbol)
                        info = pos.get("info") or {}

                        if "average_entry_price_usd" in info:
                            entry = float(info["average_entry_price_usd"])
                        else:
                            entry = pos.get("entryPrice") or pos.get("entry_price") or pos.get("averagePrice")

                        size = pos.get("contracts") or pos.get("size") or pos.get("amount")
                        side = (pos.get("side") or "").lower()

                        if entry is None or size is None:
                            print("   ⚠️ Paradex entry/size 없음 → fallback 유지")
                        else:
                            size = float(size)
                            if side in ("long", "buy"):
                                signed = abs(size)
                            elif side in ("short", "sell"):
                                signed = -abs(size)
                            else:
                                signed = abs(size) * (1 if paradex_side == "buy" else -1)

                            paradex_entry_price = float(entry)
                            paradex_pos_size = signed

                            print(
                                f"   ✅ [Paradex {para_symbol}] ENTRY 세팅: entry={paradex_entry_price:.6f}, "
                                f"size={paradex_pos_size:.6f}, side={side or paradex_side}"
                            )

                            unreal = pos.get("unrealizedPnl") or pos.get("unrealizedPnlUsd")
                            if unreal is not None:
                                try:
                                    print(f"   🔍 [DEBUG] Paradex exchange unrealizedPnl={float(unreal):+.6f}")
                                except Exception:
                                    pass
                    except Exception as e:
                        print(f"   ⚠️ [Paradex {para_symbol}] fetch_position 오류: {e}")

                    paradex_entry_needs_refresh = False

                # ============================================
                # fill-gap 검증(롤백) - ENABLE_FILLGAP_ROLLBACK=True 일 때만 실행
                # ============================================
                if ENABLE_FILLGAP_ROLLBACK:
                                    if (not fillgap_checked) and (time.time() <= fillgap_check_deadline):
                                        if lighter_entry_price and paradex_entry_price:
                                            gap_usd, gap_pct = calc_fill_gap(
                                                current_coin, direction, lighter_entry_price, paradex_entry_price
                                            )
                                            min_gap = min_fill_gap_percent(current_coin)
                    
                                            print("\n========== FILL-GAP CHECK ==========")
                                            print(f"coin={current_coin}, dir={direction}")
                                            print(f"lt_entry={lighter_entry_price:.6f} | pd_entry={paradex_entry_price:.6f}")
                                            print(f"fill_gap={gap_pct*100:.4f}% ({gap_usd:+.6f} USD/coin)")
                                            print(f"min_fill_gap={min_gap*100:.4f}% (majors/alt split)")
                                            print("====================================")
                    
                                            # 롤백 조건:
                                            # 1) fill_gap이 음수(역전) 또는
                                            # 2) fill_gap이 최소 기준 미만
                                            if gap_pct <= 0 or gap_pct < min_gap:
                                                await emergency_close_all(
                                                    reason=f"fill-gap too small/reversed: fill={gap_pct*100:.4f}% < min={min_gap*100:.4f}%"
                                                )
                                                continue
                    
                                            fillgap_checked = True
                    
                                    # deadline 지났는데도 entry 못 잡았으면(이상 케이스) 롤백
                                    if (not fillgap_checked) and (time.time() > fillgap_check_deadline):
                                        await emergency_close_all(reason="fill-gap check timeout (entry not ready)")
                                        continue

            # ============================================
            # PnL 루프
            # ============================================
            if in_position and current_coin is not None and direction is not None:
                lt_feed = lighter_feeds.get(current_coin)
                pd_feed = paradex_feeds.get(current_coin)
                if lt_feed is None or pd_feed is None:
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                lt_bid, lt_ask, _ = lt_feed.get_prices(allow_stale=ALLOW_STALE_FOR_PNL)
                pd_bid, pd_ask, _ = pd_feed.get_prices(allow_stale=ALLOW_STALE_FOR_PNL)

                if lt_bid is None or lt_ask is None or lighter_side is None:
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                lt_cur = lt_bid if lighter_side == "buy" else lt_ask

                # Lighter PnL
                lt_pnl = 0.0
                if lighter_entry_price is not None:
                    if lighter_pos_size is not None:
                        lt_pnl = (lt_cur - lighter_entry_price) * lighter_pos_size
                    else:
                        s = 1 if lighter_side == "buy" else -1
                        lt_pnl = (lt_cur - lighter_entry_price) * qty_coin * s

                # Paradex PnL
                pd_pnl: Optional[float] = None
                pd_cur: Optional[float] = None
                pd_has_price = False
                if paradex_entry_price is not None and paradex_pos_size is not None:
                    if pd_bid is not None and pd_ask is not None:
                        pd_cur = pd_bid if paradex_pos_size > 0 else pd_ask
                        pd_pnl = (pd_cur - paradex_entry_price) * paradex_pos_size
                        pd_has_price = True

                # TOTAL PnL: 두 다리 모두 현재가가 있을 때만 신뢰
                total_pnl: Optional[float] = None
                if (lt_cur is not None) and (pd_has_price or paradex_pos_size == 0):
                    total_pnl = lt_pnl + (pd_pnl or 0.0)

                elapsed = time.time() - position_entry_time

                now = time.time()
                lt_age = now - lt_feed.get_last_update_ts()
                pd_age = now - pd_feed.get_last_update_ts()

                if now - last_pnl_log > 1.0:
                    last_pnl_log = now
                    pd_pnl_str = f"{pd_pnl:+.6f}" if pd_pnl is not None else "N/A"
                    total_pnl_str = f"{total_pnl:+.6f}" if total_pnl is not None else "N/A"
                    pd_cur_str = f"{pd_cur:.6f}" if pd_cur is not None else "N/A"
                    lt_cur_str = f"{lt_cur:.6f}" if lt_cur is not None else "N/A"
                    print(
                        f"[{current_coin} PnL] Lighter≈{lt_pnl:+.6f} | Paradex={pd_pnl_str} | "
                        f"TOTAL={total_pnl_str} (target={TARGET_PROFIT_USD}) | elapsed={elapsed:.1f}s | "
                        f"snapshot_gap={(entry_gap_pct_snapshot or 0)*100:.4f}%\n"
                        f"   lt_entry={(lighter_entry_price or 0):.6f} lt_cur={lt_cur_str} ({lighter_side}) | "
                        f"pd_entry={(paradex_entry_price or 0):.6f} pd_cur={pd_cur_str} ({paradex_side})\n"
                        f"   [freshness] lt_age={lt_age:.3f}s, pd_age={pd_age:.3f}s (allow_stale={ALLOW_STALE_FOR_PNL})"
                    )
                    if (paradex_pos_size is not None and paradex_pos_size != 0) and not pd_has_price:
                        print("   ⚠️ [WARN] Paradex last price 없음 → TOTAL/청산신호 무효 (feed quiet/reconnect 가능)")

                # 목표 수익 3회 감지 → 청산
                if (total_pnl is not None) and (total_pnl >= TARGET_PROFIT_USD):
                    exit_signal_count += 1
                    print(f"⏱️ 청산 신호 감지: count={exit_signal_count}, TOTAL={total_pnl:+.6f} / target={TARGET_PROFIT_USD}")

                    if exit_signal_count >= 3:
                        print("\n🎯 목표 이익 조건 3회 감지 → 청산(동시)")

                        if direction == "LT_SHORT_PD_LONG":
                            lt_close_side = "buy"
                            pd_close_side = "sell"
                        else:
                            lt_close_side = "sell"
                            pd_close_side = "buy"

                        lt_bid2, lt_ask2, _ = lt_feed.get_prices(allow_stale=ALLOW_STALE_FOR_CLOSE)
                        pd_bid2, pd_ask2, _ = pd_feed.get_prices(allow_stale=ALLOW_STALE_FOR_CLOSE)

                        lt_close_task = asyncio.create_task(
                            create_lighter_market_order(client, market_info, current_coin, lt_close_side, qty_coin)
                        )
                        pd_close_task = asyncio.to_thread(
                            place_paradex_aggressive_limit,
                            pd, COIN_MAPPING[current_coin], pd_close_side, qty_coin, True, pd_bid2, pd_ask2
                        )

                        try:
                            lt_close_res, _ = await asyncio.gather(lt_close_task, pd_close_task)
                            print(f"   ✅ [Lighter {current_coin}] 청산 완료: {lt_close_res}")
                            print(f"💵 이번 라운드 추정 손익 합: {total_pnl:+.6f} USD\n")
                        except ParadexTokenExpired:
                            print("   ⚠️ Paradex 토큰 만료 → 클라이언트 재생성 후 청산 재시도")
                            pd = create_paradex_client()
                            pd_retry = asyncio.to_thread(
                                place_paradex_aggressive_limit,
                                pd, COIN_MAPPING[current_coin], pd_close_side, qty_coin, True, pd_bid2, pd_ask2
                            )
                            await pd_retry
                            lt_close_res = await lt_close_task
                            print(f"   ✅ [Lighter {current_coin}] 청산 완료(재시도 후): {lt_close_res}")

                        # reset
                        in_position = False
                        current_coin = None
                        direction = None
                        qty_coin = 0.0
                        lighter_side = None
                        paradex_side = None
                        lighter_entry_price = None
                        lighter_pos_size = None
                        paradex_entry_price = None
                        paradex_pos_size = None
                        entry_gap_pct_snapshot = None
                        position_entry_time = 0.0
                        exit_signal_count = 0
                        lighter_entry_needs_refresh = False
                        paradex_entry_needs_refresh = False
                        fillgap_checked = False
                        fillgap_check_deadline = 0.0

                await asyncio.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n⏹️ Ctrl+C 감지 - 긴급 종료")
        await emergency_close_all(reason="KeyboardInterrupt")

    except Exception as e:
        print(f"⚠️ 메인 루프 에러: {e}")
        import traceback
        traceback.print_exc()

    finally:
        for f in lighter_feeds.values():
            try:
                await f.stop()
            except Exception:
                pass
        for f in paradex_feeds.values():
            try:
                await f.stop()
            except Exception:
                pass
        try:
            await client.close()
        except Exception:
            pass

        try:
            if log_file and not log_file.closed:
                log_file.flush()
                log_file.close()
                print("📁 로그 파일 안전 종료")
        except Exception as e:
            print(f"⚠️ 로그 파일 종료 오류(무시): {e}")

        print("🧹 모든 연결 종료")
        print(f"📝 로그 저장 완료: {log_filename}")


async def main():
    await gap_trading_loop()

if __name__ == "__main__":
    asyncio.run(main())