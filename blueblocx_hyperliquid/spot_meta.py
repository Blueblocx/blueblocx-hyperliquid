"""
Resolve HyperCore spot coin identities from the Info API `spotMeta` response.

HyperCore spot coins appear in fills/ledger as either the canonical name
"BASE/QUOTE" (e.g. "PURR/USDC") or the non-canonical "@index" (e.g. "@207").
Neither carries decimals or a stable token identity on its own. `spotMeta` maps
each universe pair -> [baseTokenIndex, quoteTokenIndex] and each token -> its
weiDecimals + tokenId. We use it to give every spot leg:

  - real `decimals`  (so amount_raw is correct and cross-run stable)
  - a stable `token_address`  (keyed by the numeric token index, NOT the display
    symbol, so a token that gets renamed still nets correctly)
  - the QUOTE token identity, because ~5% of pairs are NOT quoted in USDC
    (verified live: 17/316 pairs). The USDC counter-leg assumption is wrong for
    those, so callers must value the counter-leg in the real quote token.

Token index 0 is USDC by convention; we keep the pre-existing synthetic address
`hlcore:usdc` for it so USDC rows stay joinable to the pinned $1 price.
"""
from __future__ import annotations

from dataclasses import dataclass

from . import schema as S


@dataclass(frozen=True)
class TokenInfo:
    index: int
    symbol: str
    decimals: int
    token_address: str
    is_stablecoin: bool
    token_id: str | None = None      # spotMeta `tokenId` (0x...): the key of Info `tokenDetails`


@dataclass(frozen=True)
class SpotPair:
    """One spot market: base token traded against a quote token."""
    coin: str            # the fill/ledger coin string ("PURR/USDC" or "@207")
    candle_coin: str     # the coin string candleSnapshot accepts (often "@index")
    base: TokenInfo
    quote: TokenInfo


# Tokens we treat as $1 stablecoins on HyperCore (priced by pinning, not candles).
_STABLE_SYMBOLS = {"USDC", "USDT0", "USDT", "USDHL", "FEUSD"}


def _token_address(index: int, symbol: str) -> str:
    if index == 0:
        return S.USDC_ADDRESS               # keep legacy synthetic USDC address
    return f"hlcore:token:{index}"


class SpotMetaResolver:
    """Wraps a spotMeta payload and resolves coin -> SpotPair / token -> TokenInfo."""

    def __init__(self, spot_meta: dict):
        self._tokens: dict[int, TokenInfo] = {}
        for t in spot_meta.get("tokens", []) or []:
            idx = int(t["index"])
            sym = t.get("name") or f"token{idx}"
            self._tokens[idx] = TokenInfo(
                index=idx,
                symbol=sym,
                decimals=int(t.get("weiDecimals", 0) or 0),
                token_address=_token_address(idx, sym),
                is_stablecoin=sym.upper() in _STABLE_SYMBOLS,
                token_id=(str(t["tokenId"]) if t.get("tokenId") else None),
            )
        # coin string -> SpotPair, indexed by BOTH canonical name and "@index".
        self._pairs: dict[str, SpotPair] = {}
        for u in spot_meta.get("universe", []) or []:
            toks = u.get("tokens") or []
            if len(toks) != 2:
                continue
            base = self._tokens.get(int(toks[0]))
            quote = self._tokens.get(int(toks[1]))
            if base is None or quote is None:
                continue
            # candleSnapshot reliably accepts the "@index" form for spot; the
            # literal "SYMBOL/USDC" 500s for many pairs. Canonical pairs also
            # accept their display name, but "@index" is universal.
            candle_coin = f"@{u['index']}"
            pair = SpotPair(coin=u.get("name", ""), candle_coin=candle_coin,
                            base=base, quote=quote)
            if u.get("name"):
                self._pairs[u["name"]] = pair                 # "PURR/USDC"
            self._pairs[f"@{u['index']}"] = pair               # "@0"

    def token(self, index: int) -> TokenInfo | None:
        return self._tokens.get(index)

    def resolve_coin(self, coin: str) -> SpotPair | None:
        """Resolve a fill/ledger spot coin string to its SpotPair, or None."""
        return self._pairs.get(coin)

    def resolve_symbol(self, symbol: str) -> TokenInfo | None:
        """Resolve a bare token symbol (as carried by `send`/`spotTransfer`
        ledger deltas, which name the token, not the pair)."""
        for t in self._tokens.values():
            if t.symbol == symbol:
                return t
        return None

    def usdc_pair_for_base(self, base_symbol: str) -> SpotPair | None:
        """The USDC-quoted spot pair for a base token symbol (for pricing a spot
        holding in USD). Returns None if the token has no USDC market."""
        for p in self._pairs.values():
            if p.base.symbol == base_symbol and p.quote.index == 0:
                return p
        return None

    def usdc_pairs(self) -> dict[str, SpotPair]:
        """base token_address -> its USDC-quoted spot pair, for every token that has
        one (the price step's universe of priceable tokens)."""
        out: dict[str, SpotPair] = {}
        for p in self._pairs.values():
            if p.quote.index == 0 and p.base.index != 0:
                out.setdefault(p.base.token_address, p)
        return out


def empty() -> "SpotMetaResolver":
    """A resolver with no data (used when spotMeta fetch fails; callers then
    fall back to symbol-keyed synthetic addresses + decimals 0)."""
    return SpotMetaResolver({"tokens": [], "universe": []})
