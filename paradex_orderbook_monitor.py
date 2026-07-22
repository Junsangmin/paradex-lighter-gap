"""
Paradex BBO 모니터링 (stale 감지 + 강제 재연결)
- 1초마다 bid/ask/mid/spread + age 출력
- stale(age > STALE_RECONNECT_SEC)면 WS를 닫고 즉시 재연결
"""

import os
import sys
import time
import asyncio
from typing import Optional
import aiohttp
from dotenv import load_dotenv

load_dotenv()

PARA_WS_URL = os.getenv("PARA_WS_URL", "wss://ws.api.prod.paradex.trade/v1")
SYMBOL = "SOL-USD-PERP"


# ===== 튜닝 포인트 =====
PRINT_ALLOW_STALE = True          # 출력은 stale도 보여줄지
FRESH_MAX_AGE_SEC = 0.50          # get_prices()에서 fresh 판정 기준
STALE_RECONNECT_SEC = 1.50        # 이 이상 업데이트 없으면 "강제 재연결"
RECV_TIMEOUT_SEC = 1.0            # ws.receive timeout
MAX_SPREAD_USD = 100.0            # 비정상 스프레드 컷


class ParadexBBOFeed:
    def __init__(self, url: str, market_symbol: str, coin: str = ""):
        self._url = url
        self._market_symbol = market_symbol
        self._channel = f"bbo.{market_symbol}"
        self._coin = coin

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

    async def wait_until_ready(self, timeout: float = 10.0):
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    def get_last_update_ts(self) -> float:
        return self._last_update_ts

    def get_prices(
        self,
        max_age_sec: float = FRESH_MAX_AGE_SEC,
        max_spread_usd: float = MAX_SPREAD_USD,
        allow_stale: bool = False,
    ):
        bid = self._bid
        ask = self._ask
        if bid is None or ask is None:
            return None, None, None

        now = time.time()
        age = now - self._last_update_ts
        if (age > max_age_sec) and not allow_stale:
            return None, None, None

        spread = ask - bid
        if spread <= 0 or spread > max_spread_usd:
            return None, None, None

        mid = (bid + ask) / 2.0
        return bid, ask, mid

    async def _run(self):
        print(f"🌐 [Paradex WS] 연결 시작: {self._coin or self._market_symbol} channel={self._channel}")

        while not self._stop:
            ws: Optional[aiohttp.ClientWebSocketResponse] = None
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self._url, heartbeat=30) as ws:
                        print(f"[Paradex WS] connected: {self._coin or self._market_symbol}")
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "method": "subscribe",
                            "params": {"channel": self._channel},
                            "id": 1,
                        }
                        await ws.send_json(sub_msg)

                        # receive 루프를 "타임아웃 기반"으로 돌려서
                        # - 메시지가 안 오면 age 체크
                        # - stale면 ws 닫고 break -> outer while에서 재연결
                        while not self._stop:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=RECV_TIMEOUT_SEC)
                            except asyncio.TimeoutError:
                                # 메시지 안 왔음 -> stale 감시
                                age = time.time() - self._last_update_ts if self._last_update_ts else 999
                                if self._ready.is_set() and age > STALE_RECONNECT_SEC:
                                    print(
                                        f"⚠️ [Paradex WS] STALE 감지: age={age:.3f}s > {STALE_RECONNECT_SEC:.2f}s "
                                        f"→ 강제 재연결"
                                    )
                                    await ws.close()
                                    break
                                continue

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

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

            except Exception as e:
                print(f"[Paradex WS] error({self._coin or self._market_symbol}): {e}")

            if not self._stop:
                print(f"[Paradex WS] 종료됨({self._coin or self._market_symbol}), 1초 후 재연결 시도")
                await asyncio.sleep(1.0)


async def main():
    print("=" * 60)
    print(f"Paradex BBO 모니터링: {SYMBOL}")
    print("=" * 60)

    feed = ParadexBBOFeed(PARA_WS_URL, SYMBOL, coin=SYMBOL)
    await feed.start()

    try:
        await feed.wait_until_ready(timeout=30.0)
        print("\n✅ WebSocket 연결 완료. 1초마다 출력 시작...\n")
    except Exception:
        print("⚠️ WebSocket 준비 타임아웃, 진행")

    last_log_time = 0.0

    try:
        while True:
            now = time.time()
            if now - last_log_time >= 1.0:
                # 출력은 stale도 보여주되, age를 함께 표시해서 "UI 불일치" 원인을 바로 보게 함
                bid, ask, mid = feed.get_prices(allow_stale=PRINT_ALLOW_STALE)

                age = now - feed.get_last_update_ts() if feed.get_last_update_ts() else 999.0
                stale_flag = "STALE" if age > FRESH_MAX_AGE_SEC else "FRESH"
                timestamp = time.strftime("%H:%M:%S", time.localtime(now))

                if bid is not None and ask is not None and mid is not None:
                    spread = ask - bid
                    spread_pct = (spread / mid * 100) if mid > 0 else 0.0
                    print(
                        f"[{timestamp}] {SYMBOL} | "
                        f"Bid: {bid:.6f} | Ask: {ask:.6f} | Mid: {mid:.6f} | "
                        f"Spread: {spread:.6f} ({spread_pct:.4f}%) | "
                        f"age={age:.3f}s {stale_flag}"
                    )
                else:
                    print(f"[{timestamp}] {SYMBOL} | 데이터 없음 | age={age:.3f}s {stale_flag}")

                last_log_time = now

            await asyncio.sleep(0.05)

    except KeyboardInterrupt:
        print("\n⏹️ Ctrl+C 감지 - 종료 중...")
    finally:
        await feed.stop()
        print("🧹 연결 종료 완료")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())



