-- ============================================================
-- ETL World Bank — DDL das tabelas de destino
-- Ordem de criação: countries -> indicators -> wdi_facts (FK)
-- ============================================================

CREATE TABLE IF NOT EXISTS countries (
    iso2_code    CHAR(2)       PRIMARY KEY,
    iso3_code    CHAR(3),
    name         VARCHAR(100)  NOT NULL,
    region       VARCHAR(80),
    income_group VARCHAR(60),
    capital      VARCHAR(80),
    longitude    NUMERIC(9,4),
    latitude     NUMERIC(9,4),
    loaded_at    TIMESTAMP     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS indicators (
    indicator_code VARCHAR(40)  PRIMARY KEY,
    indicator_name TEXT         NOT NULL,
    unit           VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS wdi_facts (
    iso2_code      CHAR(2)      REFERENCES countries(iso2_code),
    indicator_code VARCHAR(40)  REFERENCES indicators(indicator_code),
    year           SMALLINT     NOT NULL,
    value          NUMERIC(18,4),
    loaded_at      TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (iso2_code, indicator_code, year)
);
