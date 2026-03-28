import time
from extract import extract_all
from transform import transform_all
from load import load_data, get_engine


def wait_for_db(retries: int = 10, delay: int = 3) -> None:
    """Aguarda o banco aceitar conexões antes de iniciar o pipeline."""
    from sqlalchemy import text
    engine = get_engine()
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("[main] banco de dados disponível")
            return
        except Exception as exc:
            print(f"[main] aguardando banco... tentativa {attempt}/{retries} ({exc})")
            time.sleep(delay)
    raise RuntimeError("[main] não foi possível conectar ao banco após todas as tentativas")


def run_etl() -> None:
    print("[main] iniciando pipeline ETL — World Bank")

    wait_for_db()

    # ── Extração ─────────────────────────────────────────────────────────────
    countries_raw, indicators_raw = extract_all()

    print(f"[main] CHECKPOINT extração:")
    print(f"         countries_raw : {len(countries_raw)} registros")
    for code, records in indicators_raw.items():
        print(f"         {code}: {len(records)} registros")

    if not countries_raw:
        raise RuntimeError("[main] ABORTADO — extração de países retornou vazio. "
                           "Verifique conectividade com api.worldbank.org")

    if all(len(v) == 0 for v in indicators_raw.values()):
        raise RuntimeError("[main] ABORTADO — extração de indicadores retornou vazio. "
                           "Verifique conectividade com api.worldbank.org")

    # ── Transformação ─────────────────────────────────────────────────────────
    countries, indicators, facts = transform_all(countries_raw, indicators_raw)

    print(f"[main] CHECKPOINT transformação:")
    print(f"         countries  : {len(countries)} registros")
    print(f"         indicators : {len(indicators)} registros")
    print(f"         facts      : {len(facts)} registros")

    if not countries:
        raise RuntimeError("[main] ABORTADO — nenhum país passou pela transformação (T1/T2)")
    if not facts:
        raise RuntimeError("[main] ABORTADO — nenhum fato passou pela transformação (T3/T4/T5)")

    # ── Carga ─────────────────────────────────────────────────────────────────
    load_data(countries, indicators, facts)

    print("[main] pipeline ETL concluído")


if __name__ == "__main__":
    run_etl()

