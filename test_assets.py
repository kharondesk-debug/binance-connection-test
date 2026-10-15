import unittest

from assets import (
    accepted_contract, base_identity, classification, excluded_base,
    override_for, resolve_coin, validate_identity,
)


def contract(base="TEST", **changes):
    result = {"symbol": base + "USDT", "baseAsset": base, "marginAsset": "USDT",
              "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"}
    result.update(changes)
    return result


def detail(categories=None, description="", **changes):
    result = {"id": "independent", "name": "Independent", "symbol": "test",
              "categories": categories or [], "description": {"en": description}}
    result.update(changes)
    return result


def ticker(coin_id="independent", base="TEST", market="binance", **changes):
    result = {"coin_id": coin_id, "base": base, "target": "USDT",
              "market": {"identifier": market}}
    result.update(changes)
    return result


class AssetsTests(unittest.TestCase):
    def test_accepts_only_required_contract(self):
        self.assertTrue(accepted_contract(contract()))
        for key, value in (("marginAsset", "USDC"), ("quoteAsset", "USDC"),
                           ("contractType", "CURRENT_QUARTER"), ("status", "SETTLING")):
            with self.subTest(key=key):
                self.assertFalse(accepted_contract(contract(**{key: value})))
        self.assertFalse(accepted_contract(None))

    def test_exact_eight_excluded_bases(self):
        for base in ("BTC", "ETH", "XRP", "DOGE", "TRX", "HYPE", "BNB", "SOL"):
            with self.subTest(base=base):
                self.assertTrue(excluded_base(contract(base)))

    def test_ecosystem_names_do_not_exclude_another_asset(self):
        for base in ("JUP", "LDO", "CAKE", "HYPER", "DOGEAI", "SOLV"):
            with self.subTest(base=base):
                self.assertFalse(excluded_base(contract(base)))
        coin = detail(["Solana Ecosystem", "Governance"])
        self.assertEqual(classification(coin), "CRYPTO")

    def test_known_multiplier_only_and_digit_ticker_unchanged(self):
        self.assertEqual(base_identity(contract("1000SHIB")), "SHIB")
        self.assertEqual(base_identity(contract("1INCH")), "1INCH")
        self.assertEqual(base_identity(contract("1000UNKNOWN")), "1000UNKNOWN")
        self.assertIsNone(base_identity({"symbol": "SHIBUSDT"}))
        self.assertIsNone(base_identity({"baseAsset": {"bad": "value"}}))

    def test_duplicate_ticker_never_picks_by_name_or_order(self):
        coins = [{"id": "test-one", "symbol": "test", "name": "Test"},
                 {"id": "test-two", "symbol": "test", "name": "Other"}]
        self.assertIsNone(resolve_coin(contract(), coins))
        self.assertIsNone(resolve_coin(contract(), list(reversed(coins))))
        self.assertIsNone(resolve_coin(contract("MISSING"), coins))

    def test_unique_ticker_is_candidate_not_validated_identity(self):
        coin = detail()
        self.assertEqual(resolve_coin(contract(), [coin]), "independent")
        self.assertFalse(validate_identity(contract(), coin))
        self.assertFalse(validate_identity(contract(), detail(tickers=[ticker(market="uniswap_v3")])))
        self.assertTrue(validate_identity(contract(), detail(tickers=[ticker()])))

    def test_exact_binance_ticker_id_cross_link_required(self):
        for entry in (ticker(coin_id="other"), ticker(base="OTHER"), ticker(target="USDC"),
                      ticker(is_stale=True), ticker(is_anomaly=True), {"base": "TEST"}):
            with self.subTest(entry=entry):
                self.assertFalse(validate_identity(contract(), detail(tickers=[entry])))
        self.assertTrue(validate_identity(contract(), detail(tickers=[ticker(market="binance_futures")])))
        self.assertFalse(validate_identity(contract(), detail(symbol="other", tickers=[ticker()])))

    def test_verified_multiplier_resolves_and_validates_native_coin(self):
        coin = detail(id="shiba-inu", symbol="shib", tickers=[ticker(coin_id="shiba-inu", base="SHIB")])
        self.assertEqual(resolve_coin(contract("1000SHIB"), [coin]), "shiba-inu")
        self.assertTrue(validate_identity(contract("1000SHIB"), coin))
        wrong = detail(id="fake-1000shib", symbol="1000shib", tickers=[ticker(coin_id="fake-1000shib", base="1000SHIB")])
        self.assertFalse(validate_identity(contract("1000SHIB"), wrong))

    def test_explicit_verified_id_override_disambiguates(self):
        proof = {"coin_id": "known-project", "identity_verified": True,
                 "evidence_url": "https://www.binance.com/en/support/announcement/example"}
        coins = [{"id": "known-project", "symbol": "test"}, {"id": "impostor", "symbol": "test"}]
        self.assertEqual(resolve_coin(contract(), coins, {"TEST": proof}), "known-project")
        self.assertTrue(validate_identity(contract(), detail(id="known-project"), proof))
        self.assertFalse(validate_identity(contract(), detail(id="impostor"), proof))
        self.assertEqual(override_for(contract(), {"TEST": proof}), proof)

    def test_unverified_override_does_not_bypass_ambiguity(self):
        coins = [{"id": "one", "symbol": "test"}, {"id": "two", "symbol": "test"}]
        for override in ({"coin_id": "one"}, {"coin_id": "one", "identity_verified": True},
                         {"coin_id": "one", "identity_verified": True, "evidence_url": "http://example.com"}):
            with self.subTest(override=override):
                self.assertIsNone(resolve_coin(contract(), coins, {"TEST": override}))

    def test_stock_commodity_and_fiat_linked_assets_excluded(self):
        for category in ("Tokenized Stock", "Tokenized Stock Indices", "Tokenized Gold",
                         "Tokenized Silver", "Tokenized Oil", "Stablecoins", "USD Stablecoin",
                         "Tokenized Treasury Bills", "Commodity-backed Tokens"):
            with self.subTest(category=category):
                self.assertEqual(classification(detail([category])), "EXCLUDED")

    def test_wrapped_bridged_staking_restaking_derivatives_excluded(self):
        for category in ("Wrapped-Tokens", "Wrapped ETH", "Bridged Tokens", "Bridged USDC",
                         "Liquid Staking Tokens", "Liquid Restaking Tokens", "Vault Receipt Tokens"):
            with self.subTest(category=category):
                self.assertEqual(classification(detail([category])), "EXCLUDED")

    def test_broad_rwa_staking_and_ecosystem_categories_are_not_guessed(self):
        for category in ("Real World Assets (RWA)", "Ethereum Ecosystem", "BNB Chain Ecosystem",
                         "Decentralized Finance (DeFi)", "Liquid Staking", "Restaking"):
            with self.subTest(category=category):
                self.assertEqual(classification(detail([category])), "UNKNOWN")

    def test_staking_and_rwa_governance_utilities_can_pass(self):
        for category in ("Liquid Staking Governance Tokens", "Liquid Restaking Governance Tokens"):
            self.assertEqual(classification(detail([category])), "CRYPTO")
        coin = detail(["Real World Assets (RWA)"], "Independent is the governance token of a real-world asset infrastructure protocol.")
        self.assertEqual(classification(coin), "CRYPTO")

    def test_direct_description_of_derivative_is_excluded(self):
        for description in (
            "Independent is a wrapped representation of another cryptocurrency.",
            "Independent is a liquid restaking token representing staked ETH.",
            "Independent is a tokenized stock.",
            "Independent is a stablecoin pegged to the US dollar.",
            "Independent tracks the price of gold.",
        ):
            with self.subTest(description=description):
                self.assertEqual(classification(detail(description=description)), "EXCLUDED")

    def test_sector_description_does_not_exclude_governance_token(self):
        coin = detail(["Real World Assets (RWA)"],
                      "Independent is a governance token for a protocol offering a stablecoin and tokenized stock products.")
        self.assertEqual(classification(coin), "CRYPTO")

    def test_excluded_category_wins_over_generic_governance_category(self):
        self.assertEqual(classification(detail(["Governance", "Wrapped Tokens"])), "EXCLUDED")

    def test_unknown_classification_and_invalid_metadata_fail_closed(self):
        self.assertEqual(classification(detail()), "UNKNOWN")
        self.assertEqual(classification({"categories": ["Governance"]}), "UNKNOWN")
        self.assertEqual(classification(detail(categories="Governance")), "UNKNOWN")
        self.assertEqual(classification(None), "UNKNOWN")

    def test_verified_classification_override_requires_evidence_and_same_id(self):
        proof = {"coin_id": "independent", "classification": "CRYPTO", "classification_verified": True,
                 "evidence_url": "https://www.project.org/token"}
        self.assertEqual(classification(detail(), proof), "CRYPTO")
        self.assertEqual(classification(detail(id="different"), proof), "UNKNOWN")
        self.assertEqual(classification(detail(), {"classification": "CRYPTO"}), "UNKNOWN")

    def test_similarity_of_names_is_not_identity_evidence(self):
        coin = detail(name="Test", symbol="other", tickers=[ticker()])
        self.assertIsNone(resolve_coin(contract(), [coin]))
        self.assertFalse(validate_identity(contract(), coin))


if __name__ == "__main__":
    unittest.main()