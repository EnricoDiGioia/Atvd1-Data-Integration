from extract import extract_all
from transform import transform_all
from load import load_data


def run_etl() -> None:
    print("[main] iniciando pipeline ETL — World Bank")

    countries_raw, indicators_raw = extract_all()
    countries, indicators, facts  = transform_all(countries_raw, indicators_raw)
    load_data(countries, indicators, facts)

    print("[main] pipeline ETL concluído")


if __name__ == "__main__":
    run_etl()
