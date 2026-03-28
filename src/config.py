import os
import datetime
from dataclasses import dataclass, field


@dataclass
class Settings:
    # Banco de dados
    db_host:     str = os.getenv("DB_HOST", "localhost")
    db_port:     int = int(os.getenv("DB_PORT", 5432))
    db_name:     str = os.getenv("DB_NAME", "etl_db")
    db_user:     str = os.getenv("DB_USER", "etl_user")
    db_password: str = os.getenv("DB_PASSWORD", "etl_pass")

    # World Bank API
    wb_base_url:  str = os.getenv("WB_BASE_URL", "https://api.worldbank.org/v2")
    wb_per_page:  int = int(os.getenv("WB_PER_PAGE", 100))
    wb_mrv:       int = int(os.getenv("WB_MRV", 10))        # últimos N anos
    wb_max_retries: int = int(os.getenv("WB_MAX_RETRIES", 3))

    # Filtro temporal
    year_min: int = 2010
    year_max: int = field(default_factory=lambda: datetime.date.today().year)


# Indicadores obrigatórios: código -> (nome, unidade)
INDICATORS = {
    "NY.GDP.PCAP.KD":    ("PIB per capita (USD constante 2015)", "USD"),
    "SP.POP.TOTL":       ("População total",                     "Pessoas"),
    "SH.XPD.CHEX.GD.ZS": ("Gasto em saúde (% do PIB)",         "% PIB"),
    "SE.XPD.TOTL.GD.ZS": ("Gasto em educação (% do PIB)",      "% PIB"),
    "EG.ELC.ACCS.ZS":    ("Acesso à eletricidade (% da pop.)", "%"),
}


settings = Settings()
