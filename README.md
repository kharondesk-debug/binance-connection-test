# Binance 실시간 음수 펀딩비 알림 봇

Python 3.12 비동기 봇입니다. USDT 결제·호가의 TRADING PERPETUAL 계약에서 자산 종류와 시가총액/FDV를 확인하고, 실제 최신 선물 체결가격이 최근 90일 완성된 4시간봉 최저 종가의 3배 이상일 때만 신호를 보냅니다. 상장 후 정확히 168시간부터 편입하며 첫 7일 가격도 저점에 포함합니다. 주문·매매 기능이나 바이낸스 거래용 API 키는 없습니다.

- **A:** 현재 1시간 정산이고 예상 펀딩비가 음수이면 연속된 1시간 상태마다 한 번 보냅니다.
- **B:** 현재 4·8시간 정산이고 예상 펀딩비가 음수 하한 이하이면 이벤트마다 한 번 보냅니다. 하한 절댓값의 10%만큼 회복한 상태를 연속 60초 확인하면 새 이벤트를 허용합니다. 1시간 전환과 하한 변경도 독립적으로 처리합니다.
- A와 B는 독립적이며, 가격 조건의 일시적 이탈은 발송 기록을 초기화하지 않습니다. 가격이나 펀딩 조건이 뒤늦게 충족되어도 즉시 재평가합니다.
- BTC·ETH·XRP·DOGE·TRX·HYPE·BNB·SOL, 스테이블·원자재·주식 연계·래핑·브리지·스테이킹/리스테이킹 파생 자산 등을 제외합니다. 관련 생태계의 독립 유틸리티 코인은 일괄 제외하지 않습니다.

## 배포 순서

1. `binance_funding_alert_bot_v2.zip`을 새 폴더에 압축 해제합니다. 내부 파일을 기존 GitHub `binance-connection-test` 저장소의 최상단에 직접 업로드합니다. 실제 `.env`나 토큰은 업로드하지 않습니다.
2. 기존 저장소에서 연결 테스트 전용 실행 프로그램, 그 프로그램을 실행하는 예전 Dockerfile/실행 명령, 테스트 전용 의존성을 확인하여 직접 제거하거나 이 ZIP 파일로 교체합니다. 기존 저장소를 검사하지 않았으므로 기존 파일명을 단정할 수 없습니다. 다른 용도의 사용자 파일은 보존합니다. 이 봇의 시작 명령은 `python -m app`입니다.
3. Northflank에서 **Europe West(런던)**의 **Combined Service**를 선택합니다. GitHub 저장소 연결, 루트 `Dockerfile`, 루트 빌드 컨텍스트를 설정합니다. 인스턴스는 **1개**, 우선 **0.1 vCPU / 256MiB** 무료 사양을 사용하고 외부 공개 포트는 추가하지 않습니다. 기존 서비스가 중지되어 있어도 소스 업로드와 설정 준비는 가능합니다.
4. 같은 프로젝트/지역에 무료 **PostgreSQL Addon**을 직접 생성합니다. 현재 계정에서 무료 선택임을 확인하고 서비스에 연결합니다. Northflank가 제공하는 내부 접속 URI를 `DATABASE_URL` Secret으로 등록하고, 제공된 TLS 설정을 유지합니다. 봇이 필요한 테이블을 자동 생성합니다. 무료 DB를 만들 수 없다면 배포를 보류하십시오. `DATABASE_URL`은 필수이며 임시 컨테이너 파일로 대신 저장하지 않습니다.
5. 아래 필수 환경 변수를 Northflank의 Secret 환경 변수로 등록합니다. `.env.example`은 이름을 확인하는 빈 예시이며 프로그램이 `.env`를 자동으로 읽지는 않습니다.
6. Telegram의 [BotFather 안내](https://core.telegram.org/bots/features#botfather)에 따라 봇을 만들고 발급 토큰을 `TELEGRAM_BOT_TOKEN`에 등록합니다. 사용자 또는 지정 그룹에서 봇에 메시지를 보낸 뒤, 신뢰할 수 있는 개인 도구로 [Bot API getUpdates](https://core.telegram.org/bots/api#getupdates)의 `message.chat.id`를 확인하여 `TELEGRAM_CHAT_ID`에 등록합니다. 그룹 ID의 마이너스 기호도 보존합니다. 기존 webhook은 삭제하고 같은 토큰의 다른 long polling 프로세스는 종료합니다. 봇은 지정 Chat ID의 `/status`에만 응답합니다.
7. CoinGecko 계정의 Demo API 대시보드에서 Demo 키를 발급하여 `COINGECKO_API_KEY`에 등록합니다. `api.coingecko.com/api/v3`와 `x-cg-demo-api-key` 헤더를 사용합니다. 추가 유료 API 키는 필요하지 않습니다.
8. 선택 사항으로 Healthchecks.io 무료 체크를 만들고 비밀 Ping URL을 `HEALTHCHECKS_PING_URL`에 등록합니다. 주기 1분, 적절한 유예시간(예: 3분), 원하는 장애·복구 알림 채널을 설정합니다. 정상 바이낸스 데이터를 받고 DB가 정상인 동안만 약 60초마다 ping하므로 프로세스만 살아 있는 장애를 정상으로 보고하지 않습니다.
9. 빌드·배포 후 Northflank 실행 로그를 확인합니다. 첫 실행은 종목 식별과 가격 이력을 제한된 속도로 초기화하므로 즉시 모든 종목이 준비되지는 않습니다. 지정 Telegram 방에서 `/status`를 보내 DB 상태, 발견·편입 조건 통과 종목 수, 스트림 최근 수신시각, 진단 수, 대기 전송을 확인합니다.
10. 정상 상태에서 정기 Telegram 메시지가 없는 것은 의도된 동작입니다. `/status`가 정상이고 외부 체크가 유지되며 조건이 없으면 조용히 감시합니다. `UNKNOWN`, 데이터 지연, API/DB 오류는 해당 조건의 신호를 보류하는 상태입니다. 같은 지속 오류는 반복 알리지 않으며 여러 종목의 새 진단은 묶어서 전송합니다.
11. 재시작·재배포 시 **기존 DB와 동일한 DATABASE_URL**을 유지합니다. WebSocket·REST·DB 오류는 재시도하며 최신 데이터를 확보한 후 재평가합니다. 토큰/DB 접속 오류는 환경 변수를 수정한 뒤 재배포합니다. HTTP 403·451은 지역·접근 정책을 확인하고, 짧은 간격의 재시도로 해결하려 하지 않습니다.
12. 전송 실패 이벤트는 DB에 남고 복구 후 원래 감지시각과 지연 사실을 포함해 전송됩니다. 신호가 그동안 끝났더라도 이미 감지된 이벤트는 보존합니다. 정상 재시작에서 동일 이벤트 중복을 억제하려면 DB 테이블을 삭제하거나 다른 빈 DB로 바꾸지 않습니다.

## 환경 변수

| 이름 | 설정 |
| --- | --- |
| `TELEGRAM_BOT_TOKEN` | 필수: BotFather 토큰 |
| `TELEGRAM_CHAT_ID` | 필수: 허용된 대화방 ID |
| `COINGECKO_API_KEY` | 필수: Demo API 키 |
| `DATABASE_URL` | 필수: PostgreSQL 접속 URI |
| `HEALTHCHECKS_PING_URL` | 선택: 비밀 ping URL |
| `COINGECKO_MONTHLY_BUDGET` | 선택: 기본 6000회/최근 32개 UTC 날짜, 무료 한도보다 여유 있게 제한 |
| `MAX_PENDING_MESSAGES` | 선택: 기본 2000개, 영구 전송 대기열 상한 |

## 데이터 확인과 보류 기준

`exchangeInfo`와 CoinGecko 시총·FDV는 24시간마다, `fundingInfo`는 10초마다 갱신합니다. 168시간 경과는 별도 스케줄로 확인합니다. 새로 발견한 종목은 유효한 `exchangeInfo.onboardDate`를 그대로 사용합니다. 실제 상장 후 168시간이 지났다면 추가 대기는 없습니다. 명확한 재상장 증거가 있지만 새 상장시각을 검증할 수 없으면 `RELISTING_TIME_UNKNOWN`으로 보류하고, 실제 새 상장시각 확인 후 168시간 규칙을 적용합니다. 4시간봉은 최초 이력 및 새 완성봉만 가져오며 실시간 이동 90일 범위를 유지합니다. 가격은 `!miniTicker@arr`의 실제 최신 체결가격이고 펀딩비는 `!markPrice@arr@1s`의 예상값입니다. `-0.02`는 `-2%`이며 금액 비교에는 Decimal을 사용합니다.

펀딩비 15초, 가격 120초 이상 갱신이 없거나 정산 정보가 300초를 넘으면 새 신호를 보류합니다. CoinGecko 마지막 정상 확인이 48시간을 넘거나 시총·FDV가 null/0/비정상이면 보류합니다. 시총/FDV가 정확히 0.95이면 제외하며, 다시 0.95 미만이면 다른 조건을 재검사합니다.

하한은 공식 `fundingInfo` 조정값을 최우선으로 적용합니다. 조정값이 없는 일반 USDⓈ-M 계약은 공식 기본 하한 `-2%`를 쓰고, 공식 예외 종목은 `-0.75 × 최대 레버리지 구간 유지증거금률` 규칙과 구분합니다. 예외 증거금률을 얻기 위해 인증 API나 거래 키를 요구하지 않습니다. 공개 자료만으로 확정하지 못하면 마지막 검증 공식 하한을 원래 검증시각부터 최대 24시간 유지하고, 그 뒤 최근 30일 확정 펀딩비의 관측 최저 음수 값을 `추정 하한`으로 명시합니다. 유효 이력이 없으면 하한 미확인으로 B를 보류합니다. A는 하한과 독립적입니다. [Binance 공식 펀딩 규칙](https://www.binance.com/en/support/faq/detail/360033525031)

정산 주기는 공식 조정값, 공식 기본 8시간, 실제 연속 정산시각 증거 순서로 확인합니다. 공식 기본값과 최근 증거가 충돌하거나 1·4·8시간을 검증할 수 없으면 `INTERVAL_UNKNOWN`으로 보류합니다. 다음 정산까지 1시간 남았다는 사실로 1시간 주기를 추정하지 않습니다. `fundingInfo`와 `fundingRate`의 공식 공유 한도는 500회/5분/IP이며 봇은 이보다 낮은 공유 예산을 적용합니다. [Binance 공식 시장 API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)

CoinGecko ID를 먼저 식별한 뒤 Binance 거래 페어 또는 검증된 명시적 매핑으로 확인합니다. 같은 티커의 여러 자산, 불명확한 숫자 배수 계약, 확인되지 않은 분류는 자동 선택하지 않습니다. 분류 진단을 해결하려면 공식 자산 설명을 확인합니다. 필요한 경우 `assets.json`에 증거 URL과 정확한 ID를 등록하고 재배포할 수 있습니다. 다음은 형식 예시이며, `TEST`와 URL은 실제 검증값으로 바꿔야 합니다.

```json
{
  "TEST": {
    "coin_id": "exact-coingecko-id",
    "identity_verified": true,
    "evidence_url": "https://official.example/asset-identity",
    "classification": "CRYPTO",
    "classification_verified": true,
    "classification_evidence_url": "https://official.example/token-description"
  }
}
```

키는 정확한 Binance 심볼 또는 기초자산이고 심볼이 우선합니다. 분류값은 `CRYPTO`, `EXCLUDED`, `UNKNOWN`입니다. 증거 없는 `verified: true` 표시는 실제 검증을 대신하지 못합니다. CoinGecko는 래핑·브리지·LST·LRT 등 원자산 표시 토큰을 별도 자산으로 다룹니다. [CoinGecko 공식 분류 설명](https://support.coingecko.com/articles/7346603845-why-do-we-list-native-vs-non-native-cryptocurrencies-separately-on-coingecko)

## 무료 자원과 영구 상태

CoinGecko 조회는 묶어서 수행하고 분류·매핑을 캐싱합니다. 검증된 자산 식별·분류는 7일, 불확실한 결과는 3일마다 재확인하며 시총·FDV는 매일 갱신합니다. 검증된 assets.json 변경은 재배포 후 첫 갱신부터 적용됩니다. 내부 제한은 기본 20회/분과 최근 32개 UTC 날짜 버킷 합계 6000회이며 실패 요청도 예산에 포함합니다. 일별 버킷을 DB에 유지하므로 재시작하거나 달이 바뀌어도 사용량을 갑자기 초기화하지 않습니다. 환경 변수 이름은 COINGECKO_MONTHLY_BUDGET이지만 공급자의 월별 결제·집계 시작일에 의존하지 않는 보수적인 이동 구간입니다. 같은 API 키를 다른 프로그램에서도 쓴다면 그 사용량을 고려해 내부 한도를 더 낮춥니다. 기본 총예산 6000회 중 메타데이터 조회는 공통 누적 사용량 5000회부터 멈춰 시총·디렉터리 배치 조회용 1000회를 남깁니다. 메타데이터를 확보하지 못한 자산은 UNKNOWN으로 보류합니다. 한도에 도달하면 무리하게 호출하지 않고 갱신을 보류합니다. 현재 Demo의 100회/분·월 10,000회보다 낮게 제한하며 실제 계정 한도를 확인하십시오. [CoinGecko 공식 API 가격](https://www.coingecko.com/en/api/pricing)

DB에는 최신 종목 상태, 필요한 최근 완성봉, 캐시, 진단 및 현재 이벤트 ID만 갱신합니다. 성공한 임시 대기 메시지는 삭제하고 상장폐지·오래된 캐시를 정리합니다. 실시간 시세, 전체 메시지·WebSocket 이력, 로컬 로그 파일은 누적하지 않습니다. PostgreSQL의 통상 VACUUM과 사용량은 Northflank에서 확인하십시오.

대기열이 상한에 도달하면 기존 미발송 이벤트를 보존하고 새 이벤트 판정을 일시 중지하며 `/status`와 외부 감시에 장애를 표시합니다. 무한 기간의 Telegram 장애 동안 모든 새 이벤트를 보존하면서 저장공간을 고정하는 것은 불가능합니다. Telegram 성공 응답 후 DB 기록 전 프로세스가 죽거나 API 타임아웃이 전송 성공 여부를 감추는 경계에서는 재전송 중복이 발생할 수 있습니다. 영구 이벤트 ID·트랜잭션·단일 프로세스로 중복 가능성을 줄이지만 Telegram API와 DB 사이의 완전한 exactly-once 전송은 보장하지 않습니다.

확인한 Northflank 가격 페이지는 Sandbox에 무료 서비스 2개와 무료 DB 1개를 안내합니다. 실제 지역·계정별 무료 제공 여부를 배포 화면에서 확인하며 이 프로그램은 자원을 생성하거나 유료 사양을 선택하지 않습니다. 무료 Sandbox의 24시간 가용성 보증이나 256MiB에서 모든 실시간 부하를 처리한다는 실측 보장은 제공하지 않습니다. [Northflank 공식 가격](https://northflank.com/pricing), [Sandbox 제한 안내](https://northflank.com/docs/v1/application/billing/pricing-on-northflank)

## 모의 테스트와 실행

Python 3.12에서 다음을 실행합니다. 테스트는 API 키·실제 네트워크·실제 거래소/Telegram 호출 없이 모의 데이터를 사용합니다.

```text
python -m pip install -r requirements.txt
python -B -m unittest discover -v
python -m app
```

마지막 명령은 필수 환경 변수를 먼저 등록한 실제 운영 실행입니다. Docker는 `python:3.12-slim-bookworm`, 비루트 사용자, 런타임 모듈만 포함하고 공개 포트를 열지 않습니다. 컨테이너 실행 시 환경 변수를 주입합니다. 이 ZIP의 검수는 Python 문법, 모의 테스트, 시작 모듈·Dockerfile 구조를 확인합니다. 실제 Docker 이미지 빌드, 실제 PostgreSQL 접속·트랜잭션, Northflank 배포, 장시간 실연결, 실제 Telegram 전송은 배포 환경에서 별도 확인해야 합니다.

## v2 수정 사항

- 새 발견 시 실제 상장시각을 유지하며, 이전 버전의 잘못된 발견시각을 정정할 때 첫 7일을 포함한 이력을 다시 확보합니다. 기존 A/B 발송 상태는 보존합니다. 폐지·재상장 확인 메타데이터는 최대 365일·2000종목으로 제한합니다.
- 초기 CoinGecko 수집이 진행 중이면 정상 수집 대기와 실제 장애를 구분합니다. 미확인 상태는 유형별 한 개 요약으로 알리며, 같은 유형의 영향 종목 증가만으로 반복 전송하지 않습니다. 지속 문제는 초기화·재시도 중에도 유지하고, 해결 후 재발이나 새 중요한 장애는 별도 이벤트로 보냅니다.
- `/status`는 전체 계약과 TRADING 수, 제외 지정 자산, 자산연계 제외, 매칭·FDV·상장시각 미확인, FDV 95% 이상, 7일 대기와 실제 데이터 준비 수를 표시합니다. 각 제외 사유는 중복 가능하며 합계가 전체와 일치하지 않습니다. 실제 감시 가능 수는 필수 데이터 준비 기준이고 3배·음수 시그널 조건과 별개입니다.
- `/status SYMBOL`은 종목별 상세 문제를, `/status 1`, `/status 2` 등은 미확인 종목 목록을 페이지당 15개씩 표시합니다. 지정 Chat ID의 직접 요청에만 응답합니다.
- 실시간 가격과 예상 펀딩비는 메모리에서 갱신합니다. 영구 상태가 바뀐 종목만 저장하고 공식 확인시각은 공통 메타데이터 한 행으로 분당 최대 한 번 보존합니다. 하한·주기·상장정보 및 A/B 이벤트 변경은 저장하며, 이벤트 상태와 전송 대기 등록은 동일 트랜잭션입니다.
