# Binance Funding Alert Bot v4

기존 Binance 공개시장 감시 봇의 수동 승인, 정기 REST 3분, 진단 사건 억제 및 메모리 절감 버전입니다. 매매 기능과 Binance 거래 키를 사용하지 않습니다.

## 승인 정책과 데이터 보존
- approval_policy.json은 사용자가 확정한 2026-10-15 스냅샷입니다. 전체 고유 계약 525 = APPROVED 337 + BLOCKED 187 + REVIEW 1(1000RATSUSDT).
- 고정 SHA와 심볼 집합을 검증하고 최초 DB 연결에서 한 번만 bot_watch 및 approval_seed_v4 메타를 초기화합니다. 이후 텔레그램 변경을 초기 명단으로 덮어쓰지 않습니다.
- 기존 테이블/가격봉/A·B 이벤트 상태/미전송 큐를 초기화하지 않습니다. 신규 bot_watch 테이블과 bot_pending.reply_markup 컬럼은 멱등 생성/추가합니다.
- 신규 계약은 PENDING이며 상태·안내 outbox를 같은 트랜잭션에 기록합니다. 불명확한 과거 계약은 실제 신규 상장으로 단정해 대량 안내하지 않습니다. 상장시각이 확인되면 해당 신규 사건의 안내를 한 번 등록합니다.
- 재상장된 승인 종목은 PENDING 재검토, 차단 종목은 BLOCKED 유지. 같은 재상장의 시각 보완으로 안내나 버튼 버전을 다시 만들지 않습니다.
- 168시간, 90일 완성 4시간봉 최저 종가, 실제 체결가격 3배, A/B 독립성, 신선도, B의 10%·60초 재무장, 기존 이벤트 ID와 전송 재시도는 보존했습니다.
- CoinGecko를 런타임에 호출하지 않습니다. 승인 목록을 자동으로 재선별하지 않습니다.

## 설정과 텔레그램
필수 기존 설정: DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID.
선택: HEALTHCHECKS_PING_URL, MAX_PENDING_MESSAGES, TELEGRAM_ADMIN_USER_ID.
개인 채팅의 양수 CHAT_ID는 관리자 from.id로 사용합니다. 그룹/채널은 양수 TELEGRAM_ADMIN_USER_ID를 설정해야 승인·차단을 허용합니다. chat.id와 from.id를 모두 검증합니다.

명령: /status, /status SYMBOL, /status 1, /watch, /pending [페이지], /approve SYMBOL, /block SYMBOL, /review SYMBOL.
신규 안내의 ✅ 승인 / ⛔ 차단 버튼과 명령은 같은 DB 상태를 사용합니다. /review는 REVIEW로 재검토 전환하며 /pending에 함께 표시됩니다.
콜백은 세대 번호 CAS로 멱등 처리하고 DB 실패를 성공으로 표시하지 않습니다. 승인 해제·재승인 시 기존 A/B 전송 상태를 지우지 않습니다.
긴 목록은 페이지로 나눕니다. 발송 성공 시 message_id를 저장하며 원래 안내를 처리 결과로 편집합니다.

## 주기, 장애, 메모리
- EXCHANGE, FUNDING_INFO, LIVE_RECHECK만 각각 180초로 변경했습니다.
- !markPrice@arr@1s, !miniTicker@arr 실시간 수신과 수신 직후 A/B 평가는 유지합니다.
- DIAGNOSTICS는 15초, DB 점검은 5초 등 기존 주기를 유지합니다.
- 별도 2초 점검이 신선도 초과를 감지하면 REST 보완을 시작합니다. 요청 간 최소 10초, 지수 백오프, 공유 403/451 30분·429/418 Retry-After cooldown을 적용합니다. 정기 조회와 장애 복구가 같은 lock을 사용합니다.
- 정상 API에서도 공식 REST에만 나타난 정산 주기 변경은 다음 정기 조회까지 최대 약 180초 늦을 수 있습니다. API 장애/백오프 중에는 더 늦어집니다. 검증된 WS 전환 정보는 즉시 평가하고 신선도 제한은 완화하지 않습니다.
- WS의 원인별 연속 60초 장애에 1회 진단, 짧은 복구 후 재발은 억제, 30분 연속 안정 복구 후에만 새 사건을 허용합니다. PostgreSQL 사건 기록으로 재시작 후 억제 상태를 복원합니다. 단절 원인은 CLOSE/TIMEOUT/HTTP 차단/데이터 등으로 구분합니다.
- 진단 억제와 실제 데이터 신선도는 별개입니다. /status에는 실제 장애·미준비 상태가 계속 나타납니다. A/B 메시지에는 진단 억제를 적용하지 않습니다.
- 승인 종목만 SQL에서 가격봉을 선택하고 cursor prefetch=16으로 직접 State를 복원합니다. 미승인 종목의 DB 봉은 읽거나 삭제하지 않습니다. 재승인은 DB 이력을 재사용하고 빈 구간만 보충합니다.
- Candle/State/Event slots, typed-state fingerprints, 2개 이력 worker, 최대 541개 봉·750개 펀딩 이력, 필요한 펀딩 필드만 캐시하여 전체 JSON 복사를 줄입니다.
- Linux 실행 시 분당 RSS·승인 수·봉 수를 stdout에 출력합니다. 실시간 시세/로그 누적 DB를 추가하지 않습니다.

## 오프라인 검증
Python 3.12에서 requirements.txt를 설치한 환경:
    python -B -m unittest discover -q
    python -B memory_benchmark.py --baseline 7dad34c4756e32fcde5d1fa927379ec1b8e92978

단위·통합 테스트는 모의 API/DB/Telegram을 사용하며 실제 메시지를 보내지 않습니다.
메모리 비교에는 Git 이력이 있는 checkout과 git 실행 파일이 필요합니다. 필요하면 GIT_EXECUTABLE 환경 변수로 git 경로를 지정합니다. ZIP만 풀어도 unittest는 실행할 수 있습니다.
최종 오프라인 테스트 185개 통과 / 0개 실패입니다.
검증한 구성은 Python 3.12.14, aiohttp 3.14.5, asyncpg 0.32.0입니다.

Windows 격리 RSS 측정(525개 State, 종목당 최대 541봉, 보수적인 원본 펀딩 이력 750개 및 전체 WS 프레임 부하):
| 단계 | v3 | v4 |
| --- | ---: | ---: |
| 시작 RSS | 47.24 MiB | 48.05 MiB |
| 가격봉 복원 중 최대 | 199.06 MiB | 90.96 MiB |
| 안정 후·최대 | 283.79 MiB | 192.03 MiB |
최대 약 32.3% 감소. v4는 승인337개의 182,317봉, 미승인188개의 메모리 봉0입니다. 실제 v4의 축약 펀딩 캐시보다 불리한 원본 캐시로 측정했습니다.
실제 Northflank 네트워크 버퍼·256MB 환경·장시간 운영은 이 오프라인 측정과 다를 수 있으며 24시간 안정 운영을 보장하는 수치가 아닙니다.
Telegram은 요청 타임아웃 시 서버 수신 여부를 확인하는 idempotency API가 없습니다. DB의 사건·안내 등록은 고유 ID로 중복 억제하지만, 전송 성공 직후 통신/DB 연결이 끊기는 모호한 응답에서 재시도 중복 배달 가능성까지 제거한다고 주장하지 않습니다.

## 배포와 확인
Docker 시작은 python -m app, 기존 서비스 sh5651/DB funding-bot-db/Secret Group funding-bot-secrets를 사용합니다.
Docker build에는 런타임 7개 모듈, 고정 의존성, 승인 JSON만 복사하며 테스트·문서·Git·비밀값은 포함하지 않습니다.
빌드 후 시작 로그의 v4 DB restored, v4 policy, 두 WS fresh, Telegram polling을 확인하고 /status, /watch 및 RSS 로그를 확인합니다.
초기에는 승인337/차단187/보류1/대기0이 기준이며, 기준 스냅샷 이후의 실제 신규 계약 또는 사용자 변경이 있으면 총수는 달라질 수 있습니다.
이전 운영 기준 GitHub main SHA는 7dad34c4756e32fcde5d1fa927379ec1b8e92978입니다. v4의 추가 테이블·컬럼은 기존 소스가 사용하지 않으므로 이전 소스로 롤백해도 기존 테이블을 삭제하지 않습니다.
