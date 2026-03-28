import time
import requests
from typing import List, Dict, Any
from config import settings, INDICATORS


# ── Países ───────────────────────────────────────────────────────────────────

def fetch_countries() -> List[Dict[str, Any]]:
    url = f"{settings.wb_base_url}/country"
    params = {"format": "json", "per_page": 300}

    for attempt in range(settings.wb_max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            countries = data[1] if len(data) > 1 else []
            print(f"[extract] countries: {len(countries)} registros extraídos")
            return countries
        except Exception as exc:
            print(f"[extract] tentativa {attempt + 1}/{settings.wb_max_retries} países falhou: {exc}")
            time.sleep(2 ** (attempt + 1))

    raise RuntimeError("[extract] falha ao buscar países após todas as tentativas")


# ── Indicadores (série histórica com paginação) ───────────────────────────────

def fetch_indicator(indicator_code: str) -> List[Dict[str, Any]]:
    url = f"{settings.wb_base_url}/country/all/indicator/{indicator_code}"
    all_records = []
    page = 1

    while True:
        params = {
            "format":   "json",
            "per_page": settings.wb_per_page,
            "mrv":      settings.wb_mrv,
            "page":     page,
        }

        for attempt in range(settings.wb_max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                meta    = data[0]
                records = data[1] if len(data) > 1 and data[1] else []
                break
            except Exception as exc:
                print(f"[extract] tentativa {attempt + 1}/{settings.wb_max_retries} "
                      f"indicador {indicator_code} pág {page} falhou: {exc}")
                time.sleep(2 ** (attempt + 1))
                if attempt == settings.wb_max_retries - 1:
                    raise RuntimeError(
                        f"[extract] falha definitiva no indicador {indicator_code} pág {page}"
                    )

        all_records.extend(records)
        total_pages = meta.get("pages", 1)

        if page >= total_pages:
            break
        page += 1

    print(f"[extract] indicador {indicator_code}: {page} páginas, {len(all_records)} registros")
    return all_records


# ── Ponto de entrada ──────────────────────────────────────────────────────────

def extract_all():
    print("[extract] iniciando extração")

    countries_raw = fetch_countries()

    indicators_raw = {}
    for code in INDICATORS:
        print(f"[extract] extraindo indicador: {code}")
        indicators_raw[code] = fetch_indicator(code)

    print(f"[extract] extração concluída — {len(indicators_raw)} indicadores")
    return countries_raw, indicators_raw
