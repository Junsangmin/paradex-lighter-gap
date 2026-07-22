# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

이 레포지토리는 Lighter와 Paradex 거래소 간의 가격 차이(갭)를 감지하고 차익거래를 수행하는 비동기 Python 트레이딩 봇입니다.

## Core Architecture

### Trading Logic Flow

1. **WebSocket Price Feeds**: Lighter와 Paradex 양쪽 거래소의 실시간 가격을 WebSocket으로 수신
2. **Gap Calculation**: 두 거래소 간 가격 차이를 지속적으로 계산
3. **Signal Detection**: 설정된 임계값 이상의 갭이 일정 시간 유지되면 진입 신호 발생
4. **Simultaneous Execution**: 양쪽 거래소에 동시에 주문 전송 (지연 최소화)
5. **PnL Monitoring**: WebSocket 기반 실시간 PnL 계산 (API 호출 최소화)
6. **Exit Condition**: 목표 수익 달성 시 양쪽 포지션 동시 청산

### Main Script Versions

- `gap_trading.py`: 기본 갭 트레이딩 로직 (WebSocket 기반)
- `gap_trading_v2.py`: Lighter 가격 글리치 필터 추가
- `gap_trading_v3.py`: 다중 코인 지원 (BTC, ETH, SOL, SUI, BNB, HYPE)
- `gap_trading_v4.py`: 진입 신호 집계 방식 개선 (시간 윈도우 내 다수 신호 필요)
- `gap_trading_v5.py`: 로그 파일 자동 저장, 디버그 로그 추가
- `gap_trading_v6.py`: Paradex entry price 매칭 버그 수정 (오차 허용 범위 1.0 → 5%)
- `gap_trading_v7.py`: **현재 사용 버전** - 코인 그룹 시스템 (8개씩 묶음) + 동적 코인 매핑 + 실시간 웹 대시보드
  - 활성화된 그룹의 코인 중 Lighter와 Paradex 양쪽에 존재하는 것만 자동 선택
  - WebSocket 동시 연결 제한 회피 (Paradex ~10개 제한)
  - `dashboard.html`: 실시간 갭 및 포지션 정보 표시

### Key Components

#### 1. LighterWSFeed (Lighter WebSocket)
- Lighter 거래소의 order_book WebSocket 채널 구독
- bid/ask 가격 및 타임스탬프 추적
- 오래된 데이터 자동 필터링 (max_age_sec)
- 비정상 스프레드 감지 (max_spread_usd)

#### 2. ParadexBBOFeed (Paradex WebSocket)
- Paradex BBO (Best Bid/Offer) 채널 구독
- JSON-RPC 2.0 프로토콜 사용
- 자동 재연결 로직 포함

#### 3. MultiCoinGapScanner
- 여러 코인을 동시에 모니터링
- 각 코인의 양방향 갭(LONG/SHORT) 계산
- 가장 유리한 기회(최대 갭) 선택
- Lighter 가격 글리치 필터링 (v3 이상)

#### 4. Position Entry System
- Lighter: 시장가 주문 (극단 가격 + IOC)
- Paradex: 공격적 지정가 (스프레드 관통, 빠른 체결)
- asyncio.gather()로 양쪽 동시 실행

#### 5. PnL Calculation
- **Lighter PnL**: account API에서 entry price/size 조회 후 WebSocket 가격으로 실시간 계산
- **Paradex PnL**: fetch_position() API로 실제 PnL 조회 (토큰 만료 자동 처리)

#### 6. DashboardBroadcaster (v7)
- WebSocket 서버 (포트 8765)를 통해 실시간 데이터 브로드캐스트
- 갭 정보: 모든 모니터링 중인 코인의 양쪽 가격 및 갭 (실시간)
- 포지션 정보: 진입가, 현재가, PnL, 경과 시간 등 (1초마다 업데이트)
- `dashboard.html` 파일로 웹 브라우저에서 실시간 모니터링 가능

## Environment Variables

`.env` 파일에 다음 변수 필수:

### Lighter Exchange
```
API_KEY_PRIVATE_KEY=0x...
ACCOUNT_INDEX=12345
API_KEY_INDEX=2
L1_ADDRESS=0x...
LIGHTER_URL=https://mainnet.zklighter.elliot.ai
LIGHTER_WS_URL=wss://mainnet.zklighter.elliot.ai/stream
```

### Paradex Exchange
```
ETHEREUM_ADDRESS=0x...
ETHEREUM_PRIVATE_KEY=0x...
PARA_WS_URL=wss://ws.api.prod.paradex.trade/v1
```

## Running the Bot

### 메인 봇 실행
```bash
# v7 실행 (웹 대시보드 포함)
python gap_trading_v7.py

# 대시보드 열기
# 브라우저에서 http://localhost:8080 접속
# WebSocket이 자동으로 ws://localhost:8765에 연결됨
```

### 테스트 스크립트
```bash
# Lighter 단독 테스트
python test_open_position/lighter_open_position.py

# Paradex 단독 테스트
python test_open_position/paradex_open_position.py

# Backpack 테스트 (추가 거래소)
python test_open_position/backpack_open_position.py
```

## Configuration Parameters

주요 설정값 (`gap_trading_v7.py` 기준):

```python
ENTRY_GAP_PERCENT = 0.001            # 진입 갭 임계값 (0.1%)
TARGET_PROFIT_USD = 0.3              # 청산 목표 수익 ($)
POSITION_COLLATERAL_USD = 80.0       # 한쪽 증거금
LEVERAGE = 5.0                       # 레버리지
POLL_INTERVAL = 0.1                  # 메인 루프 간격 (초)
SIZE_STEP = "0.001"                  # 수량 스텝
AGGRESSIVE_SLIPPAGE = 0.001          # Paradex 슬리피지 (0.1%)
ENTRY_SIGNAL_WINDOW = 20.0           # 진입 신호 윈도우 (초)
ENTRY_SIGNAL_MIN_COUNT = 2           # 최소 신호 횟수
MIN_COIN_PRICE = 0.01                # 최소 코인 가격 (필터링)

# 코인 그룹 설정
# COIN_GROUPS에서 하나의 그룹만 enabled=True로 설정
# GROUP_1: BTC, ETH, SOL, SUI, HYPE, FARTCOIN, BNB, AVAX (8개)
```

## Critical Implementation Details

### Windows Event Loop
Windows 환경에서는 `asyncio.WindowsSelectorEventLoopPolicy()` 필수 설정

### Order Execution
- **진입**: 양쪽 거래소 동시 주문 (딜레이 최소화)
- **청산**: 양쪽 거래소 동시 청산 (reduceOnly=True for Paradex)

### Signal Validation (v4)
- 단순 임계값 초과가 아닌 시간 윈도우 내 다수 신호 집계 방식
- 가짜 신호(노이즈) 필터링 효과

### Price Glitch Filtering (v3+)
- Lighter 가격의 급격한 변화 감지 (MAX_LT_JUMP_PCT)
- 비정상 틱 무시하고 이전 값 유지

### Token Expiry Handling
Paradex PnL 조회 시 토큰 만료 감지 및 자동 재생성

### Entry Price Tracking
- **Lighter**: account API로 실제 체결 가격 조회
- **Paradex**: fetch_my_trades()로 최근 거래 내역에서 조회 (v6에서 개선)
  - ⚠️ **중요**: 오차 허용 범위를 주문 수량의 5% 이내로 설정 (v6에서 수정)
  - v5 이전: 1.0 절대값 오차 허용 → 잘못된 거래 매칭 발생 (약 $0.14 오차)
  - v6: qty * 0.05 (5% 오차) → 정확한 매칭
  - 디버그: 최근 거래 내역 5개 출력하여 매칭 과정 확인 가능
- **Fallback**: WebSocket 가격 사용

## Supported Coins

### v6 이전
`COIN_MAPPING` 딕셔너리에 하드코딩:
- BTC, ETH, SOL, SUI, BNB, HYPE 등

### v7 (현재 버전)
**동적 코인 매핑**: Lighter와 Paradex API에서 공통 마켓 자동 탐색
- `build_coin_mapping()` 함수가 자동으로 양쪽 거래소 공통 코인 감지
- 새로운 코인 추가 시 코드 수정 불필요
- 필터링 옵션:
  - `MIN_COIN_PRICE`: 최소 코인 가격 (기본값 0.01 USD)
  - `COIN_BLACKLIST`: 제외할 코인 리스트 (예: ["ZEC", "SHIB"])
- 상세 로그 출력:
  - Lighter에 없는 코인
  - Paradex에 없는 코인
  - 필터링된 코인 (가격, 블랙리스트, API 오류)

**코인 그룹 시스템** (v7.1):
- Paradex WebSocket 동시 연결 제한(~10개) 회피
- 77개 코인을 8개씩 10개 그룹으로 분할
- `COIN_GROUPS`에서 하나의 그룹만 `enabled=True`로 설정
- 간단한 설정 변경으로 다른 그룹 모니터링 가능
- 동적 매핑 프로세스:
  1. 활성화된 그룹의 코인 목록 읽기 (예: GROUP_1의 8개 코인)
  2. Lighter와 Paradex에서 전체 마켓 목록 조회
  3. 각 코인이 양쪽에 모두 존재하는지 확인
  4. 가격 필터 및 블랙리스트 체크
  5. 최종 매핑된 코인만 트레이딩에 사용

**실시간 웹 대시보드** (v7):
- `dashboard.html`: 브라우저 기반 실시간 모니터링
- 자동 연결: WebSocket (ws://localhost:8765)
- 표시 정보:
  - 모든 코인의 실시간 가격 및 갭 (테이블 형태)
  - 포지션 정보 (코인, 방향, 수량, 진입가, PnL 등)
  - 색상 코딩 (양수 PnL: 초록색, 음수: 빨간색)
  - 진입 가능한 갭 강조 표시 (펄스 애니메이션)

## Development Notes

### Testing Individual Exchanges
`test_open_position/` 폴더의 스크립트들은 각 거래소의 주문 실행 및 PnL 모니터링을 독립적으로 테스트할 수 있습니다.

### Debugging
- 모든 주요 동작에 이모지 포함 로그 출력
- GAP SNAPSHOT: 1초마다 모든 코인의 갭 정보 출력
- PnL 로그: 포지션 보유 중 지속적으로 양쪽 PnL 출력

### Risk Management
- 포지션 크기는 POSITION_COLLATERAL_USD와 LEVERAGE로 제어
- 청산 조건은 TARGET_PROFIT_USD 기준 (손절 로직 없음)
- 한 번에 하나의 포지션만 보유 (in_position 플래그)

## Code Modification Guidelines

### 새로운 코인 추가

#### v6 이전
1. `COIN_MAPPING`에 Lighter/Paradex 심볼 매핑 추가
2. 거래소 양쪽에서 해당 마켓 지원 확인

#### v7 (현재)
**자동 추가됨!** 다음 경우에만 수동 조정:
- 특정 코인 제외: `COIN_BLACKLIST`에 추가
- 저가 코인 제외: `MIN_COIN_PRICE` 조정

### 파라미터 조정
- 갭 임계값, 청산 목표, 포지션 크기 등은 실행 전 백테스팅 권장
- 슬리피지 설정은 시장 유동성에 따라 조정 필요

### WebSocket 재연결
- 각 Feed 클래스는 자동 재연결 로직 내장
- 3초 대기 후 재시도

### Error Handling
- 주문 실패 시 반대편 포지션 청산 로직 추가 고려
- 네트워크 오류는 루프 내에서 catch하고 계속 실행
