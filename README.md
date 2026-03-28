# ETL World Bank — Painel de Indicadores Socioeconômicos

## Visão Geral

Pipeline ETL que extrai dados da **World Bank Data API v2** (api.worldbank.org), transforma e carrega em um banco PostgreSQL estruturado. O objetivo é alimentar um painel comparativo de indicadores socioeconômicos entre países, permitindo análises históricas limpas e reexecutáveis.

A API é pública, gratuita e não exige autenticação. São extraídos 5 indicadores obrigatórios (PIB per capita, população, gasto em saúde, gasto em educação e acesso à eletricidade) para todos os países do mundo com dados entre 2010 e o ano corrente.

---

## Modelo de Dados

**Abordagem:** ORM com `DeclarativeBase` (SQLAlchemy 2.x).  
**Justificativa:** Permite definir os modelos como classes Python tipadas, facilitando leitura e manutenção. O upsert é feito via `pg_insert().on_conflict_do_update()`, garantindo idempotência sem SQL literal no `load.py`. O uso de psycopg2 direto (como na aula de cerveja) foi substituído pelo SQLAlchemy conforme exigência do enunciado.

```
countries                        indicators
─────────────────────────        ──────────────────────────
iso2_code  CHAR(2)   PK          indicator_code VARCHAR(40)  PK
iso3_code  CHAR(3)               indicator_name TEXT
name       VARCHAR(100)          unit           VARCHAR(30)
region     VARCHAR(80)
income_group VARCHAR(60)
capital    VARCHAR(80)                    wdi_facts
longitude  NUMERIC(9,4)         ───────────────────────────────────
latitude   NUMERIC(9,4)         iso2_code      CHAR(2)   FK → countries
loaded_at  TIMESTAMP            indicator_code VARCHAR(40) FK → indicators
                                year           SMALLINT
                                value          NUMERIC(18,4)   ← NULL ok
                                loaded_at      TIMESTAMP
                                PK: (iso2_code, indicator_code, year)
```

---

## Regras de Transformação

**T1 — Filtro de Entidade**  
Descarta registros cujo `id` do país tenha comprimento ≠ 2. A API retorna agregados regionais (`EAS`, `WLD`, etc.) com 3 letras — `len(iso2) == 2` elimina todos sem lista negra manual.

**T2 — Limpeza de Strings**  
`safe_str()` aplica `.strip()` e converte strings vazias para `None`. Nomes de região são padronizados em `title-case` para consistência em agrupamentos e filtros.

**T3 — Conversão de Tipos**  
`safe_int()` e `safe_float()` convertem `year` e `value` com `try/except`, retornando `None` em falha. Sem conversão, todos os campos chegam como strings do JSON e quebrariam cálculos numéricos.

**T4 — Filtro Temporal**  
Mantém apenas `year` entre `2010` e `datetime.date.today().year`. Evita dados históricos antigos e anos inválidos (ex: `0`, `9999`) que poderiam vir de registros corrompidos.

**T5 — Deduplicação**  
Usa um `dict` com chave `(iso2_code, indicator_code, year)` antes da carga. O último registro da iteração sobrescreve anteriores. O total de duplicatas removidas é logado.

---

## Como Executar

### Pré-requisitos
- [Docker](https://docs.docker.com/get-docker/) e Docker Compose instalados.

### Subir tudo com um comando

```bash
# 1. Clone o repositório
git clone <url-do-repositorio>
cd etl_worldbank

# 2. Copie o .env (opcional — os valores já estão no docker-compose.yml)
cp .env.example .env

# 3. Sobe o banco + executa o pipeline
docker compose up --build
```

O pipeline só inicia após o PostgreSQL responder ao `healthcheck` (`pg_isready`).  
Aguarde a mensagem: `[main] pipeline ETL concluído`

### Acessar o banco manualmente

```bash
# Conectar via psql
docker exec -it etl_postgres psql -U etl_user -d etl_db

# Ou subir apenas o banco em background
docker compose up postgres -d
```

### Re-executar o pipeline (sem derrubar o banco)

```bash
docker compose run --rm etl_app
```

### Parar tudo

```bash
# Para os containers (mantém os dados)
docker compose down

# Para os containers E apaga o volume de dados
docker compose down -v
```

---

## Consultas de Validação

Execute após a primeira execução conectado ao banco:

```bash
docker exec -it etl_postgres psql -U etl_user -d etl_db
```

### Q1 — Volume de países
```sql
SELECT COUNT(*) FROM countries;
-- Esperado: entre 200 e 220
```

### Q2 — Distribuição por grupo de renda
```sql
SELECT income_group, COUNT(*)
FROM countries
GROUP BY income_group
ORDER BY 2 DESC;
```

### Q3 — Volume e nulos por indicador
```sql
SELECT
    indicator_code,
    COUNT(*) AS obs,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS nulls
FROM wdi_facts
GROUP BY indicator_code;
```

### Q4 — PIB per capita dos 5 países de referência
```sql
SELECT c.name, f.year, f.value
FROM wdi_facts f
JOIN countries c ON c.iso2_code = f.iso2_code
WHERE f.indicator_code = 'NY.GDP.PCAP.KD'
  AND c.iso2_code IN ('BR','US','CN','DE','NG')
ORDER BY c.name, f.year;
```

### Q5 — Teste de idempotência
```sql
-- Antes da segunda execução:
SELECT COUNT(*) FROM wdi_facts;

-- Re-executar:  docker compose run --rm etl_app

-- Depois — número deve ser idêntico:
SELECT COUNT(*) FROM wdi_facts;
```

> Os resultados reais serão preenchidos aqui após a primeira execução.

---

## Decisões Técnicas

1. **Estrutura baseada na aula de ETL com Docker** (`aula_etl_docker`): padrão `dataclass` no config, funções `safe_str`/`safe_float`, separação `extract → transform → load → main`, retry com `time.sleep(2 ** attempt)`.

2. **SQLAlchemy no load.py em vez de psycopg2 direto**: exigência do enunciado. O psycopg2 continua como driver subjacente (via `postgresql+psycopg2://`), mas toda a camada de carga passa pelo SQLAlchemy Core/ORM com `pg_insert().on_conflict_do_update()`.

3. **`depends_on: condition: service_healthy`**: garante que o container ETL só inicia após o PostgreSQL responder ao healthcheck, eliminando race conditions de conexão.

4. **Upsert idempotente nas 3 tabelas**: a chave primária composta de `wdi_facts` é o mecanismo de conflito. Re-executar o pipeline N vezes produz exatamente o mesmo resultado no banco.

5. **Paginação automática em `fetch_indicator`**: o loop lê o campo `pages` do metadado de resposta da API e continua até esgotar todas as páginas, garantindo completude dos dados independente do volume.

---

## Estrutura do Projeto

```
etl_worldbank/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── db/
│   └── init.sql          ← DDL das 3 tabelas
└── src/
    ├── __init__.py
    ├── config.py          ← Settings dataclass + INDICATORS
    ├── extract.py         ← fetch_countries + fetch_indicator (paginação + retry)
    ├── transform.py       ← regras T1–T5 + safe_str/safe_float
    ├── load.py            ← upsert SQLAlchemy ORM nas 3 tabelas
    └── main.py            ← orquestrador
```
