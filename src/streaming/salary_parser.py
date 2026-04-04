
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple


# ──────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────

class Period(str, Enum):
    ANNUAL  = "annual"
    MONTHLY = "monthly"
    HOURLY  = "hourly"


HOURS_PER_YEAR = 1_820   # 35 h/week × 52 weeks


@dataclass
class ParsedSalary:
    raw:            str
    period:         Optional[Period]
    min_raw:        Optional[float]   # as stated in the string
    max_raw:        Optional[float]   # as stated in the string
    months:         Optional[float]   # e.g. 12.0 or 13.0
    annual_min:     Optional[float]   # normalized to EUR/year
    annual_max:     Optional[float]   # normalized to EUR/year
    annual_mid:     Optional[float]   # midpoint, useful for ranking/ML
    parsed_ok:      bool

    def __str__(self) -> str:
        if not self.parsed_ok:
            return f"[unparsed] {self.raw!r}"
        lo = f"{self.annual_min:,.0f}" if self.annual_min is not None else "?"
        hi = f"{self.annual_max:,.0f}" if self.annual_max is not None else "?"
        return (
            f"{self.period.value.capitalize():8s} | "
            f"raw [{self.min_raw} – {self.max_raw}] × {self.months or 1} months | "
            f"annual EUR [{lo} – {hi}]"
        )


# ──────────────────────────────────────────────
# Regex
# ──────────────────────────────────────────────

# Matches the period keyword at the start
_RE_PERIOD = re.compile(
    r"^\s*(?P<period>annuel|mensuel|horaire)",
    re.IGNORECASE,
)

# Matches one or two salary amounts
_RE_AMOUNTS = re.compile(
    r"(?P<min>\d[\d\s]*(?:[.,]\d+)?)"           # first amount (required)
    r"(?:\s*(?:Euros?|EUR|€)\s*"                 # currency after first
    r"(?:a|à|-)\s*"                              # separator
    r"(?P<max>\d[\d\s]*(?:[.,]\d+)?)"            # second amount (optional)
    r"\s*(?:Euros?|EUR|€))?",                    # currency after second
    re.IGNORECASE,
)

# Matches "sur N mois" / "sur N.0 mois"
_RE_MONTHS = re.compile(
    r"sur\s+(?P<months>\d+(?:[.,]\d+)?)\s*mois",
    re.IGNORECASE,
)


def _clean_number(s: str) -> float:
    """'36 000,50' or '36000.0' → 36000.5"""
    return float(s.replace(" ", "").replace(",", "."))

def parse_salary(libelle: Optional[str]) -> ParsedSalary:
    raw = (libelle or "").strip()

    _empty = ParsedSalary(
        raw=raw, period=None,
        min_raw=None, max_raw=None, months=None,
        annual_min=None, annual_max=None, annual_mid=None,
        parsed_ok=False,
    )

    if not raw:
        return _empty

    m_period = _RE_PERIOD.search(raw)
    if not m_period:
        return _empty

    period_str = m_period.group("period").lower()
    period = {
        "annuel":   Period.ANNUAL,
        "mensuel":  Period.MONTHLY,
        "horaire":  Period.HOURLY,
    }[period_str]

    m_amounts = _RE_AMOUNTS.search(raw)
    if not m_amounts:
        return _empty

    try:
        min_raw = _clean_number(m_amounts.group("min"))
    except (TypeError, ValueError):
        return _empty

    max_raw_str = m_amounts.group("max")
    max_raw = _clean_number(max_raw_str) if max_raw_str else None

    # If only one figure, min == max
    effective_max = max_raw if max_raw is not None else min_raw

    # ── Months ────────────────────────────────
    m_months = _RE_MONTHS.search(raw)
    months = _clean_number(m_months.group("months")) if m_months else None

    # ── Normalize to annual EUR ───────────────
    annual_min, annual_max = _to_annual(period, min_raw, effective_max, months)

    annual_mid = (
        round((annual_min + annual_max) / 2, 2)
        if annual_min is not None and annual_max is not None
        else None
    )

    return ParsedSalary(
        raw=raw,
        period=period,
        min_raw=min_raw,
        max_raw=max_raw,
        months=months,
        annual_min=round(annual_min, 2) if annual_min is not None else None,
        annual_max=round(annual_max, 2) if annual_max is not None else None,
        annual_mid=annual_mid,
        parsed_ok=True,
    )


# ── Plausibility thresholds ──────────────────────────────────────────────────
# France Travail sometimes writes annual totals in the "Mensuel" or "Horaire"
# fields (e.g. "Mensuel de 58000.0 Euros à 72000.0 Euros sur 12.0 mois").
# A real monthly salary rarely exceeds ~12 000 EUR; a real hourly rate rarely
# exceeds 500 EUR/h.  When the raw figure exceeds these caps we assume the
# value is already annual and return it unchanged.
_MONTHLY_ANNUAL_THRESHOLD = 12_000   # EUR/month — above this → treat as annual
_HOURLY_ANNUAL_THRESHOLD  =    500   # EUR/hour  — above this → treat as annual


def _to_annual(
    period: Period,
    min_val: float,
    max_val: float,
    months: Optional[float],
) -> Tuple[Optional[float], Optional[float]]:
    """Convert raw figures to annual EUR."""

    if period == Period.ANNUAL:
        # "sur N mois" is informational only when already annual
        return min_val, max_val

    if period == Period.MONTHLY:
        # Plausibility guard: real monthly salary is almost never > 12 000 EUR.
        # If it is, the figure is almost certainly already annual.
        if min_val > _MONTHLY_ANNUAL_THRESHOLD:
            return min_val, max_val
        m = months if months else 12.0
        return min_val * m, max_val * m

    if period == Period.HOURLY:
        # Plausibility guard: real hourly rate is almost never > 500 EUR/h.
        if min_val > _HOURLY_ANNUAL_THRESHOLD:
            return min_val, max_val
        # months is rare for hourly; use standard 1,820 h/year
        return min_val * HOURS_PER_YEAR, max_val * HOURS_PER_YEAR


# ──────────────────────────────────────────────
# Convenience: parse a full offer dict
# ──────────────────────────────────────────────

def parse_offer_salary(offer: dict) -> ParsedSalary:
    """
    Pass a raw France Travail offer dict.
    Reads offer["salaire"]["libelle"] safely.
    """
    libelle = (offer.get("salaire") or {}).get("libelle")
    return parse_salary(libelle)


# ──────────────────────────────────────────────
# Tests / demo
# ──────────────────────────────────────────────

if __name__ == "__main__":
    samples = [
        "Annuel de 36000.0 Euros a 40000.0 Euros sur 12.0 mois",
        "Annuel de 45000 Euros",
        "Mensuel de 2200.00 Euros sur 13 mois",
        "Mensuel de 1800 Euros a 2100 Euros sur 12 mois",
        "Horaire de 14.5 Euros a 18.0 Euros",
        "Horaire de 22,50 Euros",
        "Annuel de 60 000 Euros a 80 000 Euros",   # space-separated thousands
        # Abnormal: annual figure stated inside Mensuel/Horaire block
        "Mensuel de 58000.0 Euros à 72000.0 Euros sur 12.0 mois",
        "Horaire de 70000.0 Euros à 90000.0 Euros sur 12.0 mois",
        # Unparseable
        "",                                         # empty → parsed_ok=False
        "À négocier selon profil",                  # unparseable → parsed_ok=False
    ]

    print("=" * 70)
    for s in samples:
        result = parse_salary(s)
        print(result)
    print("=" * 70)