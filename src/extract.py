import time
import requests
from typing import List, Dict, Any, Tuple
from config import settings, INDICATORS



def _get_with_retry(url: str, params: dict) -> Any:
    """
    Faz GET com retry e backoff exponencial.
    Lança RuntimeError se todas as tentativas falharem.
    Sempre retorna o JSON parseado.
    """
    last_exc = None
    for attempt in range(settings.wb_max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_exc = exc
            wait = 2 ** (attempt + 1)
            print(f"[extract] tentativa {attempt + 1}/{settings.wb_max_retries} falhou "
                  f"({url}): {exc} — aguardando {wait}s")
            time.sleep(wait)

    raise RuntimeError(f"[extract] falha após {settings.wb_max_retries} tentativas: {last_exc}")


# ── Países ───────────────────────────────────────────────────────────────────

def fetch_countries() -> List[Dict[str, Any]]:
    url = f"{settings.wb_base_url}/country"
    params = {"format": "json", "per_page": 300}

    data = _get_with_retry(url, params)

    if not isinstance(data, list) or len(data) < 2 or not data[1]:
        print("[extract] countries: resposta inesperada da API")
        return []

    countries = data[1]
    print(f"[extract] countries: {len(countries)} registros extraídos")
    return countries


# ── Indicadores (série histórica com paginação) ───────────────────────────────

def fetch_indicator(indicator_code: str) -> List[Dict[str, Any]]:
    url = f"{settings.wb_base_url}/country/all/indicator/{indicator_code}"
    all_records: List[Dict[str, Any]] = []
    page = 1
    total_pages = 1  

    while page <= total_pages:
        params = {
            "format":   "json",
            "per_page": settings.wb_per_page,
            "mrv":      settings.wb_mrv,
            "page":     page,
        }

        data = _get_with_retry(url, params)

        # Valida estrutura da resposta
        if not isinstance(data, list) or len(data) < 2:
            print(f"[extract] {indicator_code} pág {page}: resposta inesperada — abortando paginação")
            break

        meta    = data[0] or {}
        records = data[1] or []   

        total_pages = int(meta.get("pages", 1))

        if not records:
            print(f"[extract] {indicator_code} pág {page}/{total_pages}: sem registros")
            break

        all_records.extend(records)
        print(f"[extract] {indicator_code} pág {page}/{total_pages}: "
              f"{len(records)} registros (total acumulado: {len(all_records)})")

        page += 1

    print(f"[extract] indicador {indicator_code}: {total_pages} páginas, "
          f"{len(all_records)} registros no total")
    return all_records


# ── Ponto de entrada ──────────────────────────────────────────────────────────

def extract_all() -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    print("[extract] iniciando extração")

    countries_raw = fetch_countries()

    indicators_raw: Dict[str, List[Dict[str, Any]]] = {}
    for code in INDICATORS:
        print(f"[extract] extraindo indicador: {code}")
        indicators_raw[code] = fetch_indicator(code)

    print(f"[extract] extração concluída — {len(indicators_raw)} indicadores")
    return countries_raw, indicators_raw
