# Procesy ELT w architekturze medalionowej

## 1) Problem Statement (cel analityczny)
Celem jest zbudowanie pipeline’u ELT dla danych transakcyjnych, który:
- ładuje dane surowe do bazy (warstwa Bronze),
- czyści i standaryzuje rekordy (Silver),
- udostępnia gotowe agregaty do analityki biznesowej (Gold).

Przykładowe pytania analityczne:
- Jak zmienia się wolumen transakcji dziennie?
- Które kanały mają najwyższy udział w wartości transakcji?
- Jakie MCC generują największą wartość obrotu?

## 2) Database Architecture
**ERD:** `docs/diagrams/erd.drawio`  
**High Level Architecture:** `docs/diagrams/architecture.drawio`

Opis warstw:
- **Bronze:** surowe dane z Kaggle załadowane do Postgres.
- **Silver:** dane oczyszczone (usunięte duplikaty i puste klucze).
- **Gold:** agregaty i modele gotowe do raportów.

## 3) Wymagania ELT (checklist)
- ✅ **Database created** – Postgres zdefiniowany w `docker-compose.yml`
- ✅ **Raw table loaded** – `bronze` w `src/bronze.py`
- ✅ **Cleaned table created** – `silver` w `src/silver.py`
- ✅ **Gold layer with JOIN or aggregation** – agregaty + `gold_channel_share` (JOIN)
- ✅ **Reproducible SQL script** – `sql/elt_pipeline.sql`
- ✅ **3 data quality risks** – lista poniżej

## 4) Data Quality Risks (3 kluczowe ryzyka)
1. **Duplikaty transakcji** – ten sam `transaction_id` może pojawić się wielokrotnie.  
   *Mitigacja:* `DISTINCT ON (transaction_id)` w Silver.

2. **Brakujące/ puste klucze** – `transaction_id` bywa NULL lub pusty.  
   *Mitigacja:* filtr `WHERE transaction_id IS NOT NULL AND != ''`.

3. **Problemy z formatem czasu** – `transaction_timestamp` może być stringiem lub mieć błędny format.  
   *Mitigacja:* wymuszenie typu `TIMESTAMPTZ` w Bronze i walidacja w Silver.

## 5) Uruchomienie
1. `docker compose up -d`
2. `python -m src.main`

## 6) Repozytorium
Kod pipeline’u znajduje się w `src/`, a zapytania SQL w `sql/`.
