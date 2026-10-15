"""Conservative Binance contract / CoinGecko identity and asset classification.

A unique ticker is only a candidate. Call validate_identity with /coins/{id}
(tickers=true) before using it. Never infer identity from a similar asset name.
"""
from __future__ import annotations

import html
import json
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from urllib.parse import urlsplit

EXCLUDED_BASES = frozenset({"BTC", "ETH", "XRP", "DOGE", "TRX", "HYPE", "BNB", "SOL"})
CRYPTO = "CRYPTO"
EXCLUDED = "EXCLUDED"
UNKNOWN = "UNKNOWN"

# Binance explicitly documents a contract of 1,000 SHIB and its SHIB/USDT index:
# https://www.binance.com/en-NG/support/announcement/detail/fa934b93fea6494fb7361d45ca7cc98a
# No general digit stripping: 1INCH (and unknown numeric symbols) keep their identity.
VERIFIED_MULTIPLIERS = {"1000SHIB": "SHIB"}
BINANCE_MARKETS = frozenset({"binance", "binance_futures"})

# These identify the represented asset itself, rather than a protocol sector.
# CoinGecko distinguishes non-native wrapped/bridged/LST/LRT assets from utility
# and governance tokens: https://support.coingecko.com/articles/7346603845-why-do-we-list-native-vs-non-native-cryptocurrencies-separately-on-coingecko
_EXCLUDED_CATEGORIES = frozenset({
    "tokenized stock", "tokenized stocks", "tokenized equity", "tokenized equities",
    "tokenized assets", "tokenized commodities", "commodity backed tokens",
    "commodity backed stablecoin", "commodity backed stablecoins",
    "gold backed tokens", "tokenized gold", "tokenized silver", "tokenized oil",
    "tokenized bonds", "tokenized treasury bills", "tokenized treasuries",
    "tokenized stock indices", "tokenized stock index", "tokenized index funds",
    "tokenized exchange traded funds", "tokenized exchange traded products (etps)",
    "stablecoins", "stablecoin", "fiat backed stablecoin", "fiat backed stablecoins",
    "usd stablecoin", "eur stablecoin", "gbp stablecoin", "jpy stablecoin",
    "algorithmic stablecoin", "algorithmic stablecoins",
    "crypto backed stablecoin", "crypto backed stablecoins",
    "wrapped tokens", "bridged tokens", "liquid staking tokens",
    "liquid restaking tokens", "staking receipt tokens", "restaking receipt tokens",
    "lp tokens", "vault receipt tokens",
})
_INDEPENDENT_CATEGORIES = frozenset({
    "layer 1 (l1)", "layer 2 (l2)", "meme", "meme coins", "governance", "governance tokens",
    "liquid staking governance tokens", "liquid restaking governance tokens",
})
_VARIANT_CATEGORY = re.compile(
    r"^(?:bridged|wrapped) (?:btc|wbtc|bitcoin|eth|weth|ethereum|sol|solana|bnb|avax|"
    r"usdc|usdt|dai|frax|matic)(?: tokens)?$"
)


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _category(value: object) -> str:
    return re.sub(r"\s+", " ", _text(value).casefold().replace("-", " ").replace("_", " "))


def accepted_contract(row: Mapping) -> bool:
    """Accept only active, USDT-margined and USDT-quoted perpetual futures."""
    if not isinstance(row, Mapping):
        return False
    return all(row.get(key) == expected for key, expected in (
        ("marginAsset", "USDT"), ("quoteAsset", "USDT"),
        ("contractType", "PERPETUAL"), ("status", "TRADING"),
    ))


def base_identity(row: Mapping) -> str | None:
    """Use explicit exchange metadata and only a documented multiplier alias."""
    if not isinstance(row, Mapping):
        return None
    raw = _text(row.get("baseAsset")).upper()
    if not raw or not re.fullmatch(r"[A-Z0-9_]{1,64}", raw):
        return None
    return VERIFIED_MULTIPLIERS.get(raw, raw)


def excluded_base(row: Mapping) -> bool:
    return base_identity(row) in EXCLUDED_BASES


def override_for(row: Mapping, overrides: Mapping | None) -> dict | None:
    """Most-specific symbol override wins, then raw base and verified alias."""
    if not isinstance(row, Mapping) or not isinstance(overrides, Mapping):
        return None
    for key in (_text(row.get("symbol")), _text(row.get("baseAsset")), base_identity(row)):
        if key and isinstance(overrides.get(key), Mapping):
            return dict(overrides[key])
    return None


def _evidence(override: Mapping | None, key: str = "evidence_url") -> bool:
    if not isinstance(override, Mapping):
        return False
    url = _text(override.get(key)) or _text(override.get("evidence_url"))
    try:
        parts = urlsplit(url)
        return parts.scheme == "https" and bool(parts.hostname) and not parts.username
    except ValueError:
        return False


def _verified_override(override: Mapping | None) -> bool:
    return (isinstance(override, Mapping) and override.get("identity_verified") is True
            and bool(_text(override.get("coin_id"))) and _evidence(override))


def load_overrides(path: str | Path) -> dict:
    """Load user-maintained evidence. Invalid files fail loudly, never bypass filters."""
    with Path(path).open(encoding="utf-8") as handle:
        result = json.load(handle)
    if not isinstance(result, dict):
        raise ValueError("assets.json must be an object keyed by Binance baseAsset or symbol")
    if any(not isinstance(key, str) or not isinstance(value, dict) for key, value in result.items()):
        raise ValueError("Every assets.json entry must be an object")
    return result


def resolve_coin(row: Mapping, coins_list: Iterable, overrides: Mapping | None = None) -> str | None:
    """Return a candidate ID; a separate detail identity check is mandatory.

    Duplicate tickers fail closed even when one candidate has the larger market
    cap or a more similar name. An explicit, documented mapping may disambiguate.
    """
    override = override_for(row, overrides)
    if _verified_override(override):
        return _text(override.get("coin_id"))
    base = base_identity(row)
    if base is None or not isinstance(coins_list, Iterable) or isinstance(coins_list, (str, bytes, Mapping)):
        return None
    ids = {
        _text(coin.get("id")) for coin in coins_list
        if isinstance(coin, Mapping) and _text(coin.get("symbol")).casefold() == base.casefold()
        and _text(coin.get("id"))
    }
    return next(iter(ids)) if len(ids) == 1 else None


def validate_identity(row: Mapping, coin_detail: Mapping, override: Mapping | None = None) -> bool:
    """Cross-link an exact CoinGecko ID to a Binance market or verified mapping.

    CoinGecko detail must be fetched with tickers=true. A name, unverified ticker,
    market-cap ranking or symbol-only detail is insufficient identity evidence.
    """
    if not isinstance(row, Mapping) or not isinstance(coin_detail, Mapping):
        return False
    coin_id = _text(coin_detail.get("id"))
    if not coin_id:
        return False
    if _verified_override(override):
        return _text(override.get("coin_id")) == coin_id
    base = base_identity(row)
    if not base or _text(coin_detail.get("symbol")).casefold() != base.casefold():
        return False
    tickers = coin_detail.get("tickers")
    if not isinstance(tickers, list):
        return False
    allowed_bases = {base, _text(row.get("baseAsset")).upper()}
    for ticker in tickers:
        if not isinstance(ticker, Mapping):
            continue
        market = ticker.get("market")
        if not isinstance(market, Mapping) or market.get("identifier") not in BINANCE_MARKETS:
            continue
        if ticker.get("is_anomaly") is True or ticker.get("is_stale") is True:
            continue
        if (_text(ticker.get("coin_id")) == coin_id
                and _text(ticker.get("base")).upper() in allowed_bases
                and _text(ticker.get("target")).upper() == "USDT"):
            return True
    return False


def _description(detail: Mapping) -> str:
    value = detail.get("description", "")
    if isinstance(value, Mapping):
        value = value.get("en", "")
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", _text(value)))).casefold()


def _asset_statements(detail: Mapping, description: str) -> list[str]:
    """Only statements referring to this asset, not every project in its sector."""
    names = [_text(detail.get("name")).casefold(), _text(detail.get("symbol")).casefold()]
    sentences = re.split(r"[.!?]\s+", description)
    result = []
    for sentence in sentences:
        head = sentence.lstrip(" \"'(")
        subjects = [name for name in names if name] + ["the token", "this token", "the coin", "this asset", "it"]
        for subject in subjects:
            if not head.startswith(subject):
                continue
            tail = head[len(subject):]
            # Enforce a word boundary; e.g. ON must not identify the subject ONDO.
            if tail and (tail[0].isalnum() or tail[0] == "_"):
                continue
            # Optional parenthesized ticker and direct asset predicate only.
            tail = re.sub(r"^\s*\([^)]{1,64}\)", "", tail).strip()
            if re.match(r"^(?:is|are|represents|tracks|mirrors|provides|serves|acts)\b", tail):
                result.append(tail)
            break
    return result


def classification(coin_detail: Mapping, override: Mapping | None = None) -> str:
    """Classify direct asset representations; otherwise require positive evidence.

    Broad RWA, DeFi, ecosystem, staking or restaking categories are insufficient
    by themselves. They neither exclude a utility token nor confirm its type.
    """
    if not isinstance(coin_detail, Mapping) or not _text(coin_detail.get("id")):
        return UNKNOWN
    if (isinstance(override, Mapping) and override.get("classification_verified") is True
            and _text(override.get("coin_id")) == _text(coin_detail.get("id"))
            and _evidence(override, "classification_evidence_url")):
        chosen = override.get("classification")
        if chosen in {CRYPTO, EXCLUDED, UNKNOWN}:
            return chosen
    raw_categories = coin_detail.get("categories", [])
    if not isinstance(raw_categories, list):
        return UNKNOWN
    categories = {_category(value) for value in raw_categories if _text(value)}
    if categories & _EXCLUDED_CATEGORIES or any(_VARIANT_CATEGORY.fullmatch(value) for value in categories):
        return EXCLUDED
    statements = _asset_statements(coin_detail, _description(coin_detail))
    for statement in statements:
        # A protocol's products must not redefine its independent governance
        # token. Limit this description to the token's direct predicate before
        # purpose/ownership clauses (e.g. "governance token for a stablecoin").
        direct = statement
        if re.match(r"^(?:is|serves as|acts as)\s+(?:(?:an?|the)\s+)?(?:native|utility|governance)\s+(?:token|coin|cryptocurrency)\b", direct):
            direct = re.split(r"\s+(?:for|of|within|used in|used by|used for)\s+", direct, maxsplit=1)[0]
        if re.search(
            r"\b(?:wrapped|bridged)\s+(?:version|representation|token|asset)\b|"
            r"\b(?:liquid staking|liquid restaking|staking receipt|restaking receipt)\s+(?:token|asset)\b|"
            r"\btokeni[sz]ed\s+(?:stock|share|equity|bond|commodity|gold|silver|oil|treasury|index)\b|"
            r"\bstablecoin\b|\b(?:pegged to|tracks the price of|mirrors the price of)\b|"
            r"\b(?:represents|backed by|redeemable for)\s+(?:a\s+)?(?:claim on\s+)?(?:staked|restaked)\b",
            direct,
        ):
            return EXCLUDED
    if categories & _INDEPENDENT_CATEGORIES:
        return CRYPTO
    for statement in statements:
        if re.search(r"\b(?:native|utility|governance)\s+(?:token|coin|cryptocurrency)\b", statement):
            return CRYPTO
    return UNKNOWN