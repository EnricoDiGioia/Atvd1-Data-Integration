from typing import List, Dict, Any, Optional
from config import settings, INDICATORS


# ── Helpers (mesmo padrão do transforme.py da aula) ──────────────────────────

def safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# ── T1: Filtro de entidade ────────────────────────────────────────────────────
# Países reais têm exatamente 2 caracteres no campo id.
# Agregados regionais (EAS, WLD...) têm 3+ caracteres e são descartados.

def is_real_country(iso2: Optional[str]) -> bool:
    if not iso2:
        return False
    return len(iso2.strip()) == 2


# ── Transformação de países ───────────────────────────────────────────────────

def transform_countries(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transformed = []
    skipped = 0

    for record in raw:
        iso2 = safe_str(record.get("id"))

        # T1 — descarta agregados regionais
        if not is_real_country(iso2):
            skipped += 1
            continue

        # T2 — limpeza de strings; região em title-case
        region_raw = safe_str(record.get("region", {}).get("value"))
        region = region_raw.title() if region_raw else None

        income = safe_str(record.get("incomeLevel", {}).get("value"))

        # T3 — conversão de tipos para lat/lon
        row = {
            "iso2_code":    iso2,
            "iso3_code":    safe_str(record.get("iso2Code")),
            "name":         safe_str(record.get("name")) or "Unknown",
            "region":       region,
            "income_group": income,
            "capital":      safe_str(record.get("capitalCity")),
            "longitude":    safe_float(record.get("longitude")),
            "latitude":     safe_float(record.get("latitude")),
        }
        transformed.append(row)

    print(f"[transform] countries: {len(transformed)} aceitos, {skipped} descartados (agregados)")
    return transformed


# ── Transformação de indicadores (dimensão estática do config) ────────────────

def transform_indicators() -> List[Dict[str, Any]]:
    rows = [
        {"indicator_code": code, "indicator_name": name, "unit": unit}
        for code, (name, unit) in INDICATORS.items()
    ]
    print(f"[transform] indicators: {len(rows)} indicadores preparados")
    return rows


# ── Transformação de fatos ────────────────────────────────────────────────────

def transform_facts(
    indicators_raw: Dict[str, List[Dict[str, Any]]],
    valid_iso2: set,
) -> List[Dict[str, Any]]:
    all_facts = []
    total_raw = 0

    for code, records in indicators_raw.items():
        total_raw += len(records)

        for record in records:
            iso2 = safe_str(record.get("country", {}).get("id"))

            # T1 — descarta agregados e países fora do escopo
            if not is_real_country(iso2) or iso2 not in valid_iso2:
                continue

            # T3 — conversão de tipos
            year  = safe_int(record.get("date"))
            value = safe_float(record.get("value"))   # None é permitido (campo nullable)

            # T4 — filtro temporal: apenas entre 2010 e ano corrente
            if year is None or not (settings.year_min <= year <= settings.year_max):
                continue

            all_facts.append({
                "iso2_code":      iso2,
                "indicator_code": code,
                "year":           year,
                "value":          value,
            })

    # T5 — deduplicação por (iso2, indicator_code, year), mantém o mais recente
    seen = {}
    for row in all_facts:
        key = (row["iso2_code"], row["indicator_code"], row["year"])
        seen[key] = row

    duplicates = len(all_facts) - len(seen)
    print(f"[transform] wdi_facts: {total_raw} brutos → {len(seen)} após filtros")
    print(f"[transform] duplicatas removidas (T5): {duplicates}")

    return list(seen.values())


# ── Ponto de entrada ──────────────────────────────────────────────────────────

def transform_all(countries_raw, indicators_raw):
    print("[transform] iniciando transformação")

    countries   = transform_countries(countries_raw)
    indicators  = transform_indicators()

    valid_iso2  = {row["iso2_code"] for row in countries}
    facts       = transform_facts(indicators_raw, valid_iso2)

    print("[transform] transformação concluída")
    return countries, indicators, facts
