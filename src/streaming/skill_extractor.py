from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional
from .canonical_pattern import _CANONICAL, _MULTIWORD_PATTERNS, _SINGLEWORD_PATTERNS, ROLLUP_MAP

def _normalise(label: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation noise."""
    label = label.strip().lower()
    label = re.sub(r"\s+", " ", label)
    label = re.sub(r"[^\w\s#+./]", "", label)   # keep #, +, ., / (C#, C++, Node.js, ASP.NET…)
    return label

# Build a reverse lookup: canonical_display → canonical_display (identity)
# so we can also canonicalise API skill labels
for _raw, _display in list(_CANONICAL.items()):
    _CANONICAL[_display.lower()] = _display


def canonicalise(token: str) -> str:
    """Return the canonical display name, or title-case the token if unknown."""
    return _CANONICAL.get(_normalise(token), token.strip())


def apply_rollup(
    counts: dict[str, int],
) -> tuple[dict[str, int], dict[str, str]]:
    rolled_counts: dict[str, int] = {}
    rolled_up: dict[str, str] = {}

    for skill, weight in counts.items():
        parent = ROLLUP_MAP.get(skill)
        if parent:
            rolled_up[skill] = parent
            rolled_counts[parent] = rolled_counts.get(parent, 0) + weight
        else:
            rolled_counts[skill] = rolled_counts.get(skill, 0) + weight

    return rolled_counts, rolled_up

# Compile: multi-word first, then single-word (longest-match ordering)
_COMPILED_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?<!\w)" + p + r"(?!\w)", re.IGNORECASE)
    for p in _MULTIWORD_PATTERNS + _SINGLEWORD_PATTERNS
]


def _scan_text(text: str) -> list[str]:
    """Return deduplicated canonical skill names found in free text."""
    found: dict[str, int] = {}   # canonical_name → start position (for dedup)
    for pattern in _COMPILED_PATTERNS:
        for m in pattern.finditer(text):
            canonical = canonicalise(m.group(0))
            # Keep first occurrence position (leftmost)
            if canonical not in found:
                found[canonical] = m.start()
    # Return in document order
    return [k for k, _ in sorted(found.items(), key=lambda x: x[1])]


# ──────────────────────────────────────────────
# Output type
# ──────────────────────────────────────────────

@dataclass
class ExtractedSkills:
    offer_id:     Optional[str]
    api_skills:   list[str]      = field(default_factory=list)
    text_skills:  list[str]      = field(default_factory=list)
    all_skills:   list[str]      = field(default_factory=list)  # after rollup, api first
    skill_counts: dict[str, int] = field(default_factory=dict)  # after rollup
    rolled_up:    dict[str, str] = field(default_factory=dict)  # child → parent audit

    def __str__(self) -> str:
        lines = [
            f"Offer : {self.offer_id or '—'}",
            f"API   : {', '.join(self.api_skills) or '(none)'}",
            f"Text  : {', '.join(self.text_skills) or '(none)'}",
            f"Union : {', '.join(self.all_skills) or '(none)'}",
        ]
        if self.rolled_up:
            merges = ", ".join(f"{c}→{p}" for c, p in self.rolled_up.items())
            lines.append(f"Merged: {merges}")
        return "\n".join(lines)


# ──────────────────────────────────────────────
# Main extraction function
# ──────────────────────────────────────────────

def extract_skills(offer: dict) -> ExtractedSkills:
    offer_id = offer.get("id")

    # ── 1. API skills ────────────────────────
    raw_competences = offer.get("competences") or []
    api_skills: list[str] = []
    for comp in raw_competences:
        libelle = (comp.get("libelle") or "").strip()
        if libelle:
            api_skills.append(canonicalise(libelle))
    api_skills = _dedup(api_skills)

    # ── 2. Free-text skills ──────────────────
    description = offer.get("description") or ""
    text_skills = _scan_text(description)

    # ── 3. Raw counts (pre-rollup) ───────────
    # API skills count as 2 (authoritative), text skills as 1
    raw_counts: dict[str, int] = {}
    for s in api_skills:
        raw_counts[s] = raw_counts.get(s, 0) + 2
    for s in text_skills:
        raw_counts[s] = raw_counts.get(s, 0) + 1

    # ── 4. Apply rollup ──────────────────────
    rolled_counts, rolled_up = apply_rollup(raw_counts)

    # ── 5. Rebuild all_skills from rolled counts ──
    # Preserve api-first ordering: walk api_skills, then text_skills,
    # mapping each to its rolled parent (or itself), deduplicating.
    def _rolled(skill: str) -> str:
        return ROLLUP_MAP.get(skill, skill)

    seen: set[str] = set()
    all_skills: list[str] = []
    for s in api_skills + text_skills:
        parent = _rolled(s)
        if parent not in seen:
            seen.add(parent)
            all_skills.append(parent)

    return ExtractedSkills(
        offer_id=offer_id,
        api_skills=api_skills,
        text_skills=text_skills,
        all_skills=all_skills,
        skill_counts=rolled_counts,
        rolled_up=rolled_up,
    )


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


# Demo
if __name__ == "__main__":
    sample_offer = {
        "id": "123ABC",
        "competences": [
            {"libelle": "Python"},
            {"libelle": "Développement de solutions applicatives"},
            {"libelle": "Docker"},
            {"libelle": "kubernetes"},
            {"libelle": "Apache Kafka"},
            {"libelle": "git"},
        ],
        "description": (
            "Nous recherchons un ingénieur back-end maîtrisant Python (Django, FastAPI) "
            "et les bases de données PostgreSQL et Redis. "
            "Vous travaillerez avec AWS (S3, Lambda, EC2), déploierez via Docker et Kubernetes, "
            "et maintiendrez nos pipelines CI/CD sous GitLab CI. "
            "Une expérience avec Apache Kafka, Terraform et Grafana est un plus. "
            "Méthodes Agile/Scrum. Tests unitaires avec pytest."
        ),
    }

    result = extract_skills(sample_offer)
    print(result)
    print()
    print("Skill counts after rollup (sorted by weight):")
    print(f"  {'Skill':<32} {'Weight':>6}  Source")
    print("  " + "─" * 50)
    for skill, weight in sorted(result.skill_counts.items(), key=lambda x: -x[1]):
        if weight >= 3:
            src = "API+text"
        elif weight == 2:
            src = "API"
        else:
            src = "text"
        # Flag parents that absorbed children
        children = [c for c, p in result.rolled_up.items() if p == skill]
        note = f"  ← absorbed: {', '.join(children)}" if children else ""
        print(f"  {skill:<32} {weight:>6}  [{src}]{note}")