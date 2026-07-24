# paradex-lighter-gap

Lighter ↔ Paradex 두 무기한선물 거래소 간 **가격갭 평균회귀 차익거래** 봇입니다.
asyncio 단일 이벤트 루프에서 양쪽 실시간 체결가를 받아 방향별 체결가능 갭을 계산하고,
갭이 임계값을 넘으면 저평가 거래소를 롱·고평가 거래소를 숏으로 델타중립 진입한 뒤 갭이 좁혀지면 청산합니다.

> 포트폴리오용 정리본입니다. 정본 스크립트는 `gap_trading_v13_last_trade_fixed_v2.py` 하나이며,
> 이전 버전(v7~v14)은 정리 과정에서 제거했습니다. 아래 서술의 임계값·파라미터·파일명은 모두 이 스크립트에 문자 그대로 들어 있는 값입니다.

## 개요

같은 코인(BTC/ETH/SOL/HYPE)의 무기한선물이 Lighter와 Paradex에서 서로 다른 가격에 거래되는 순간을 잡아
**두 거래소에 동시에 반대 방향 포지션**을 잡습니다. 두 다리의 방향이 반대이므로 시세 방향에 대한 순노출은 없고,
수익은 오로지 **두 거래소 가격차(갭)의 수렴**에서 나옵니다. 펀딩 차익이 아니라 순수 가격갭 회귀 전략입니다.

- **진입**: 방향별 체결가능 갭(한쪽 bid vs 반대쪽 ask)이 코인별 임계값을 넘으면 즉시 진입
  - `LT_SHORT_PD_LONG`: Lighter를 bid에 매도 + Paradex를 ask에 매수 (`lt_bid - pd_ask`)
  - `LT_LONG_PD_SHORT`: Lighter를 ask에 매수 + Paradex를 bid에 매도 (`pd_bid - lt_ask`)
  - 매 스캔에서 두 방향·전 코인 중 갭 비율(%)이 가장 큰 하나만 선택
- **청산**: 양다리 합산 PnL이 목표치(`TARGET_PROFIT_USD = 0.25`)를 **3회 연속** 넘으면 양쪽 동시 청산. 손절 로직은 없음
- **동시성**: 진입·청산 모두 Lighter(비동기)와 Paradex(`ccxt`, `asyncio.to_thread`)를 `asyncio.gather`로 병렬 전송
- **한 번에 한 포지션만** 보유 (`in_position` 플래그)

## 실행

```bash
# 의존성 (requirements 파일은 없음 — 아래 패키지를 직접 설치)
pip install aiohttp ccxt python-dotenv lighter

# 정본 봇 실행 (실계좌 주문이 나가므로 키 설정 필수)
python gap_trading_v13_last_trade_fixed_v2.py

# Paradex 피드 단독 점검용 (주문 없음, SOL-USD-PERP BBO만 출력)
python paradex_orderbook_monitor.py
```

- Python 3.9+ (`typing`, `asyncio.to_thread` 사용). Windows에서는 `asyncio.WindowsSelectorEventLoopPolicy()`를 코드가 자동 설정합니다.
- 실행 시마다 `trading_log_YYYYMMDD_HHMMSS_v7_4.txt` 로그 파일이 생성되어 콘솔 출력과 동일 내용을 기록합니다.
- 필수 라이브러리: `aiohttp`(WS), `ccxt`(Paradex 주문/조회), `python-dotenv`, `lighter`(Lighter 파이썬 SDK — `SignerClient`, `AccountApi`).

### 환경변수

`.env.example`을 `.env`로 복사한 뒤 값을 채웁니다. 실제 키는 커밋하지 않습니다. 봇 시작 시
`API_KEY_PRIVATE_KEY`와 `ACCOUNT_INDEX`가 비어 있으면 즉시 종료됩니다. 스크립트가 실제로 읽는 값은 다음과 같습니다.

```dotenv
# Lighter
API_KEY_PRIVATE_KEY=발급받은_API_키_프라이빗키
ACCOUNT_INDEX=계정_인덱스
API_KEY_INDEX=2
LIGHTER_URL=https://mainnet.zklighter.elliot.ai            # 기본값 내장
LIGHTER_WS_URL=wss://mainnet.zklighter.elliot.ai/stream    # 기본값 내장

# Paradex
ETHEREUM_ADDRESS=지갑_주소
ETHEREUM_PRIVATE_KEY=지갑_프라이빗키
PARA_WS_URL=wss://ws.api.prod.paradex.trade/v1             # 기본값 내장
```

리포지토리에 커밋된 `.env.example`은 범용 템플릿이라 이 스크립트가 쓰지 않는 항목(`BACKPACK_*`, `RAILS_TOKEN`)이 섞여 있고,
반대로 봇이 반드시 필요로 하는 `API_KEY_PRIVATE_KEY`는 빠져 있습니다. 위 목록이 v13 실제 소비 기준입니다.

## 구조

- `gap_trading_v13_last_trade_fixed_v2.py` — **정본**. 설정 상수, 4종 WS 피드 클래스, 진입/PnL/청산 메인 루프 전부 포함
  - `LighterLastTradeFeed` / `ParadexLastTradeFeed` — **실제 사용되는 피드**. 호가창이 아니라 체결(trade) 스트림을 구독해 최근 체결가로 합성 bid/ask를 만든다 (아래 "가격 피드" 참고)
  - `LighterWSFeed` / `ParadexBBOFeed` — 호가창(order_book/BBO) 기반 피드 클래스. 코드에 정의돼 있으나 메인 루프는 위의 last-trade 피드를 인스턴스화하며 이 둘은 현재 미사용
  - `UpdateNotifier` — WS 이벤트를 큐(maxsize=1)로 합쳐(coalesce) 폴링이 아닌 이벤트 구동 스캔을 가능케 함
  - `create_lighter_market_order` — Lighter IOC 시장가(가격 트릭으로 즉시 체결 유도)
  - `place_paradex_aggressive_limit` / `calc_aggressive_limit_price` — Paradex 스프레드 관통 지정가
  - `calc_fill_gap` — 실제 체결가 기준 갭 계산 (롤백 검증용)
- `paradex_orderbook_monitor.py` — Paradex BBO 단독 모니터. 1초마다 bid/ask/mid/spread/age 출력, `STALE_RECONNECT_SEC = 1.50` 초과 시 WS 강제 재연결. 피드 stale(지연) 관찰용 진단 도구
- `dashboard.html` — 이전 버전(v7)용 실시간 대시보드. `ws://localhost:8765`에 접속하도록 되어 있으나 **v13에는 해당 브로드캐스트 서버가 없어 현재 연결 대상이 없다** (레거시)
- `.env.example` — 환경변수 템플릿 (플레이스홀더만)
- `CLAUDE.md` — 구버전(v7 계열) 기준 아키텍처 노트. 일부 서술은 현재 v13과 어긋나므로 코드를 우선함

## 핵심 설계 / 방법론

### 1. 체결가능 갭을 크로스 가격으로 계산

갭은 mid-mid 차이가 아니라 **실제로 넘겨서 체결해야 하는 크로스 가격**으로 계산합니다 (`gap_trading_loop` 스캔부).

```python
gap_short_usd = lt_bid - pd_ask   # LT sell@bid, PD buy@ask
gap_long_usd  = pd_bid - lt_ask   # LT buy@ask,  PD sell@bid
gap_short_pct = gap_short_usd / lt_bid
gap_long_pct  = gap_long_usd  / lt_ask
```

한쪽은 bid, 반대쪽은 ask를 쓰므로 갭 수치 자체에 **양쪽 스프레드를 관통하는 비용이 이미 차감**되어 있습니다.
즉 여기서 나오는 갭은 "mid 기준 겉보기 괴리"가 아니라 "지금 양쪽을 시장가로 쳤을 때 남는 순갭"에 가깝습니다.
이것이 이 전략의 비용 인식 방식이며, 임계값은 이 순갭 위에 두는 **하한선** 역할을 합니다.

### 2. 진입 임계값 — 자산군 분리 고정 임계값

임계값은 시장 유동성이 다른 메이저와 알트를 나눠 하드코딩되어 있습니다.

```python
MAJOR_COINS = {"BTC"}
MAJOR_ENTRY_GAP_PERCENT = 0.0005   # 0.05% (BTC)
ALT_ENTRY_GAP_PERCENT   = 0.001    # 0.10% (그 외)

def required_entry_gap_percent(coin):
    return MAJOR_ENTRY_GAP_PERCENT if coin in MAJOR_COINS else ALT_ENTRY_GAP_PERCENT
```

- 스프레드가 얇고 왕복 체결비용이 낮은 BTC는 0.05%, 그 밖(ETH/SOL/HYPE)은 0.10%를 요구합니다.
  임계값 자체는 **실시간으로 측정한 왕복 체결비용의 배수가 아니라 자산군별 고정 상수**이며, 위 (1)의 크로스 가격 계산이 비용을 반영하는 부분입니다.
- 진입은 **NO CONFIRM** 방식입니다. 조건을 만족하는 순간 즉시 진입하며(`# ✅ NO CONFIRM: 갭 조건 만족 즉시 진입`),
  구버전에 있던 "시간 윈도우 내 다수 신호 집계" 확인 단계는 v13에서 제거되었습니다.

### 3. 스냅샷 시차 가짜갭 방어

두 거래소 피드의 갱신 시각이 어긋난 채로 갭을 재면, 실제로는 존재하지 않는 "스냅샷 시차 가짜갭"을 잡게 됩니다.
v13은 이를 **양쪽 피드 동시 신선도 게이트**로 막습니다.

```python
FRESH_MAX_AGE_SEC = 0.15
...
lt_ts = lt_feed.get_last_update_ts()
pd_ts = pd_feed.get_last_update_ts()
if (now - lt_ts) > FRESH_MAX_AGE_SEC or (now - pd_ts) > FRESH_MAX_AGE_SEC:
    continue   # 한쪽이라도 0.15초 넘게 조용하면 그 코인은 스캔에서 제외
```

- 진입 스캔에서 `get_prices(..., allow_stale=False)`로 호출하므로, 0.15초 이내에 양쪽이 모두 갱신된 코인만 후보가 됩니다.
- 한쪽 가격이 오래된 상태에서 만들어진 갭(가짜갭의 전형)은 진입 후보 단계에서 걸러집니다.
- PnL 계산·청산 단계에서는 `ALLOW_STALE_FOR_PNL = True`, `ALLOW_STALE_FOR_CLOSE = True`로 신선도 조건을 완화합니다.
  즉 "새 포지션을 여는" 판단은 엄격하게, 이미 연 포지션의 청산은 데이터가 잠깐 끊겨도 진행되도록 비대칭 설계입니다.

### 4. 체결가 기준 fill-gap 롤백 (구현되어 있으나 기본 비활성)

스냅샷으로 진입을 잡아도, 실제 체결가로 다시 계산한 갭(fill-gap)이 너무 작거나 역전됐다면 그 진입은 "가짜갭에 물린" 것입니다.
이를 잡기 위한 즉시 롤백 로직이 들어 있습니다.

```python
MAJOR_MIN_FILL_GAP_PERCENT = 0.0005
ALT_MIN_FILL_GAP_PERCENT   = 0.001
ENABLE_FILLGAP_ROLLBACK = False        # ← 기본 비활성
FILL_GAP_CHECK_TIMEOUT_SEC = 5.0
```

- 진입 직후 Lighter는 account API의 `avg_entry_price`, Paradex는 `fetch_position`의 `average_entry_price_usd`로 **실제 체결가**를 조회하고,
  `calc_fill_gap`으로 두 실체결가의 갭을 다시 계산합니다.
- `gap_pct <= 0`(역전)이거나 최소 기준 미만이면 `emergency_close_all()`로 양다리를 즉시 청산(롤백)합니다.
- 단, `ENABLE_FILLGAP_ROLLBACK = False`이므로 **현재 배포 설정에서 이 검증은 실행되지 않습니다.** 경로만 준비된 상태입니다.

### 5. 주문 실행 방식

- **Lighter**: IOC 시장가. 매도 시 `best_bid * (1 - 0.002)` 가격을 넣어 즉시 체결을 유도하고, 매수 시 극단값(`2^63-1`)을 넣습니다. (마켓 정책상 순수 시장가 대신 "시장가 흉내" 지정가+IOC)
- **Paradex**: 스프레드 관통 공격적 지정가. `AGGRESSIVE_SLIPPAGE = 0.001`(0.1%)만큼 반대편 호가를 지나쳐 던져 빠른 체결을 노립니다. 청산 시 `reduceOnly=True`.
- **포지션 크기**: `POSITION_COLLATERAL_USD = 80.0` × `LEVERAGE = 10.0` = 다리당 명목 800 USD. 수량은 `SIZE_STEP = "0.001"` 단위로 내림.
- **토큰 관리**: Paradex 토큰은 300초마다 예방적으로 재발급하고, 주문 중 만료(`INVALID_TOKEN`)를 감지하면 클라이언트를 재생성해 재시도합니다.

### 가격 피드: last-trade 합성 bid/ask

v13의 실제 피드는 호가창이 아니라 **체결 스트림**입니다.

- Lighter: `trade/{market_id}` 구독. 체결의 `is_maker_ask`로 공격자 방향을 추정 — `True`면 taker BUY이므로 그 가격을 ask, `False`면 taker SELL이므로 bid로 간주.
- Paradex: `trades.{symbol}` 구독. `side`가 `BUY`면 ask, `SELL`이면 bid로 간주.
- 한쪽 방향 체결만 들어와 bid/ask 중 하나가 비면 `last`로 메워 스프레드 0을 허용합니다.

이 방식은 호가창 API 부담 없이 가볍지만, **합성 bid/ask가 실제 최우선호가(BBO)와 다를 수 있다**는 점이 아래 한계와 직결됩니다.

## 한계 / 주의

- **표시·계산된 갭이 반드시 동시 체결 가능한 갭은 아닙니다.** 갭은 last-trade로 합성한 bid/ask에서 나오며, 이 합성값은 실제 호가창 BBO와 어긋날 수 있습니다. 특히 한쪽만 체결돼 `last`로 스프레드를 메운 구간에서는 겉보기 갭이 실제 체결 가능성을 과대평가할 수 있습니다.
- **슬리피지·체결 미끄러짐이 순갭을 잠식합니다.** 진입 임계값(0.05%/0.10%)은 얇습니다. Lighter IOC와 Paradex 관통 지정가(0.1%)의 실제 체결가가 스냅샷과 벌어지면, 진입 시점의 순갭이 사라지거나 음수가 될 수 있습니다. 이를 되돌릴 fill-gap 롤백은 존재하지만 기본 비활성(`ENABLE_FILLGAP_ROLLBACK = False`)입니다.
- **거래 수수료·펀딩은 손익 계산에 반영되지 않습니다.** PnL은 `(현재가 − 진입가) × 수량` 순수 가격차만 계산하며, 양쪽 거래소 수수료나 보유 중 펀딩은 빠져 있습니다. 목표 `$0.25`는 이 비용 차감 전 값입니다.
- **손절이 없습니다.** 청산은 목표이익 도달(3회 연속)로만 트리거됩니다. 갭이 좁혀지지 않고 벌어지는 방향으로 가면 포지션이 목표 도달까지 무기한 유지되며, 강제 종료(Ctrl+C)나 예외 발생 시에만 `emergency_close_all`로 정리됩니다.
- **양다리 비대칭 체결 리스크.** 진입/청산은 `asyncio.gather`로 동시에 보내지만 두 거래소의 실제 체결 시점은 다릅니다. 한쪽만 체결되면 순간적으로 델타중립이 깨지며, v13에는 "한쪽 실패 시 반대편 자동 정리" 로직이 없습니다.
- **`dashboard.html`은 현재 동작하지 않습니다.** v13에는 `ws://localhost:8765` 브로드캐스트 서버가 없어 대시보드는 연결 대상이 없는 레거시 파일입니다.
- **실계좌·실주문 코드입니다.** 페이퍼트레이딩 모드가 없으므로 키를 넣고 실행하면 실제 주문이 나갑니다. 파라미터는 실행 전 충분한 검증을 전제로 합니다.
