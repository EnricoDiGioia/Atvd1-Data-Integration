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

### Q1

Numero retornado foi 296

### Q2

"High income"	86
"Aggregates"	79
"Upper middle income"	54
"Lower middle income"	50
"Low income"	25
"Not classified"	2

### Q3

"SE.XPD.TOTL.GD.ZS"	2660	990
"NY.GDP.PCAP.KD"	2660	115
"SP.POP.TOTL"	    2660	10
"SH.XPD.CHEX.GD.ZS"	2660	474
"EG.ELC.ACCS.ZS"	2660	30

### Q4

"Brazil"	2015	8936.1956
"Brazil"	2016	8577.8428
"Brazil"	2017	8628.2521
"Brazil"	2018	8722.3353
"Brazil"	2019	8771.4395
"Brazil"	2020	8435.0105
"Brazil"	2021	8799.2284
"Brazil"	2022	9032.0838
"Brazil"	2023	9288.0259
"Brazil"	2024	9566.7441
"China"	    2015	8175.3329
"China"	    2016	8679.3770
"China"	    2017	9221.5140
"China"	    2018	9798.6529
"China"	    2019	10356.4804
"China"	    2020	10573.6420
"China"	    2021	11469.5707
"China"	    2022	11830.5984
"China"	    2023	12484.1579
"China"	    2024	13121.6770
"Germany"	2015	41929.7549
"Germany"	2016	42516.9337
"Germany"	2017	43543.4807
"Germany"	2018	43905.8550
"Germany"	2019	44235.2659
"Germany"	2020	42372.8727
"Germany"	2021	44011.0195
"Germany"	2022	44817.1316
"Germany"	2023	44368.9920
"Germany"	2024	44027.7632
"Nigeria"	2015	2585.7336
"Nigeria"	2016	2481.8149
"Nigeria"	2017	2441.7124
"Nigeria"	2018	2431.7786
"Nigeria"	2019	2431.5353
"Nigeria"	2020	2228.6863
"Nigeria"	2021	2206.6641
"Nigeria"	2022	2254.2908
"Nigeria"	2023	2280.9193
"Nigeria"	2024	2324.6488
"United States"	2015	56572.9189
"United States"	2016	57151.4708
"United States"	2017	58151.7021
"United States"	2018	59526.6657
"United States"	2019	60750.9899
"United States"	2020	59194.6665
"United States"	2021	62680.2504
"United States"	2022	63886.1317
"United States"	2023	65186.5977
"United States"	2024	66356.1707

### Q5

Antes: 13300
Depois: 13300
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
