from typing import List, Dict, Any

from sqlalchemy import create_engine, Column, String, Numeric, SmallInteger, Text, func
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings


# ── Engine ────────────────────────────────────────────────────────────────────

def get_engine():
    url = (
        f"postgresql+psycopg2://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )
    return create_engine(url, echo=False, pool_pre_ping=True)


# ── Modelos ORM (DeclarativeBase — SQLAlchemy 2.x) ────────────────────────────

class Base(DeclarativeBase):
    pass


class Country(Base):
    __tablename__ = "countries"

    iso2_code    = Column(String(2),   primary_key=True)
    iso3_code    = Column(String(3))
    name         = Column(String(100), nullable=False)
    region       = Column(String(80))
    income_group = Column(String(60))
    capital      = Column(String(80))
    longitude    = Column(Numeric(9, 4))
    latitude     = Column(Numeric(9, 4))


class Indicator(Base):
    __tablename__ = "indicators"

    indicator_code = Column(String(40), primary_key=True)
    indicator_name = Column(Text,       nullable=False)
    unit           = Column(String(30))


class WdiFact(Base):
    __tablename__ = "wdi_facts"

    iso2_code      = Column(String(2),  primary_key=True)
    indicator_code = Column(String(40), primary_key=True)
    year           = Column(SmallInteger, primary_key=True)
    value          = Column(Numeric(18, 4))


# ── Upsert em lote ────────────────────────────────────────────────────────────

def _upsert(session: Session, model, rows: List[Dict], conflict_cols: List[str], update_cols: List[str]):
    if not rows:
        print(f"[load] nenhum registro para {model.__tablename__}")
        return

    stmt = pg_insert(model).values(rows)
    update_set = {col: getattr(stmt.excluded, col) for col in update_cols}
    update_set["loaded_at"] = func.now()

    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_cols,
        set_=update_set,
    )
    session.execute(stmt)
    print(f"[load] {model.__tablename__}: {len(rows)} registros processados (upsert)")


# ── Carga por tabela ──────────────────────────────────────────────────────────

def load_countries(session: Session, rows: List[Dict]):
    with session.begin():
        _upsert(session, Country, rows,
                conflict_cols=["iso2_code"],
                update_cols=["iso3_code", "name", "region", "income_group",
                             "capital", "longitude", "latitude"])


def load_indicators(session: Session, rows: List[Dict]):
    with session.begin():
        _upsert(session, Indicator, rows,
                conflict_cols=["indicator_code"],
                update_cols=["indicator_name", "unit"])


def load_facts(session: Session, rows: List[Dict]):
    with session.begin():
        _upsert(session, WdiFact, rows,
                conflict_cols=["iso2_code", "indicator_code", "year"],
                update_cols=["value"])


# ── Ponto de entrada ──────────────────────────────────────────────────────────

def load_data(countries: List[Dict], indicators: List[Dict], facts: List[Dict]) -> None:
    if not countries and not facts:
        print("[load] nenhum dado para carregar")
        return

    engine = get_engine()

    try:
        with Session(engine) as session:
            # Ordem obrigatória: countries → indicators → wdi_facts (FK)
            load_countries(session, countries)
            load_indicators(session, indicators)
            load_facts(session, facts)

        print(f"[load] carga concluída com sucesso")
    except Exception as exc:
        raise RuntimeError(f"[load] falha na carga: {exc}") from exc
