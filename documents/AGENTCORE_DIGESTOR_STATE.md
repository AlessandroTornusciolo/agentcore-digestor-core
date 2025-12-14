# AGENTCORE_DIGESTOR_STATE.md

## 1. Overview

**AgentCore Digestor** è un agente di ingestion dati costruito su **AWS Bedrock AgentCore** con architettura **tool-driven**, progettato per:

- ingerire file forniti dall’utente via S3
- archiviare sempre il raw originale
- normalizzare e validare i dati
- caricare dati tabulari puliti in **Iceberg (Athena/Glue)**
- gestire formati multipli in modo controllato e dichiarativo

Il progetto è **AWS-only**, non usa Snowflake o Databricks, ed è orientato a:
- robustezza
- tracciabilità
- ripetibilità
- assenza di “magia implicita”

---

## 2. Principi Architetturali Fondamentali

Questi principi sono **non negoziabili**:

1. **Tool-driven only**
   - L’agente non processa mai direttamente i file
   - Ogni azione è delegata a un tool o a una Lambda

2. **Raw first**
   - Ogni richiesta di ingestion salva SEMPRE una copia RAW del file originale

3. **Single Source of Truth**
   - Dopo la normalizzazione, **solo il file normalizzato può essere ingerito**
   - Né il file originale né quello convertito sono mai caricati in Iceberg

4. **Pipeline deterministica**
   - L’ingestion segue sempre la stessa sequenza
   - Nessun salto di step, nessuna scorciatoia

5. **Fail fast, explain clearly**
   - Se qualcosa non è supportato, l’agente:
     - lo spiega
     - non forza il caricamento
     - non inventa soluzioni

---

## 3. Bucket S3 e Ruoli

### Upload (input utente)

agentcore-digestor-upload-raw-dev

- File caricati manualmente dall’utente
- Usato come input iniziale
- Contiene anche:
  - `converted/`
  - `normalized/`

---

### Archive (RAW storico)

agentcore-digestor-archive-dev

Struttura:

<extension>/<YYYY-MM-DD>/<filename>


- Contiene **sempre** il file originale
- Serve per audit, rollback, riprocessamento

---

### Iceberg Warehouse

agentcore-digestor-iceberg-bronze-dev


Struttura:

warehouse/<table_name>/data/


- Contiene SOLO Parquet puliti
- Scrittura tramite Lambda `load_into_iceberg`
- Mai file sporchi o raw

---

## 4. Pipeline Canonica di Ingestion

Questa è la **pipeline ufficiale**.  
Qualsiasi nuova feature deve rispettarla.

detect_file_type
→ raw_ingest
→ convert_semi_tabular (se necessario)
→ analyze_schema
→ validate_data
→ schema_normalizer
→ load_into_iceberg (normalized_path ONLY)
→ create_iceberg_table


---

## 5. Tool: Stato e Responsabilità

### detect_file_type
- Determina:
  - formato (csv, tsv, txt, xlsx, json, ndjson, pdf…)
  - classe (tabular / semi-tabular / non-tabular)
- NON legge i dati
- NON converte

---

### raw_ingest
- Copia **sempre** il file originale nell’archive bucket
- Non interpreta il contenuto
- È obbligatorio per ogni ingestion

---

### convert_semi_tabular
Converte formati non direttamente ingestibili:

| Formato | Output |
|-------|-------|
| JSON array | NDJSON |
| XLSX/XLS | CSV |
| TXT | CSV (delimiter autodetect) |
| CSV/TSV | passthrough |

Scrive in:

agentcore-digestor-upload-raw-dev/converted/


---

### analyze_schema
- Analizza **solo file tabulari**
- Richiede sempre:
  - `file_s3_path`
  - `file_format`
- Inferisce schema grezzo

---

### validate_data
- Controlla coerenza row-level
- Non rimuove righe
- Serve solo come segnale diagnostico

---

### schema_normalizer  **(CRITICO)**
- Inferisce tipo finale con majority rule
- Converte valori
- **Rimuove righe invalide**
- Scrive un CSV pulito in:

agentcore-digestor-upload-raw-dev/normalized/<filename>_normalized.csv


Restituisce:
- `normalized_path`
- `schema_normalized`
- numero righe rimosse

👉 **Il normalized file è la SINGLE SOURCE OF TRUTH**

---

### load_into_iceberg
- Tool → Lambda dockerizzata
- Legge SOLO il file normalizzato
- Scrive Parquet nel warehouse Iceberg
- Non filtra dati (si fida del normalizer)

---

### create_iceberg_table
- Crea la tabella Iceberg se non esiste
- Usa schema normalizzato
- Non carica dati

---

## 6. Stato Attuale per Formato

| Formato | Stato |
|------|------|
| CSV | ✅ completo |
| TXT | ✅ completo |
| XLSX/XLS | ✅ completo |
| JSON array | ⚠️ policy non definitiva |
| NDJSON | ⚠️ analisi OK, ingestion non definitiva |
| PDF/DOC | ✅ solo RAW + descrizione |

---

## 7. JSON: Stato e Scelte Aperte

Attuale:
- JSON array → convertibile a NDJSON
- Analisi schema funziona
- Ingestion **non ancora garantita**

Decisioni future possibili:
- Convertire JSON → CSV → pipeline standard
- Supportare NDJSON direttamente in Iceberg
- Limitare JSON a sola analisi + archiviazione

⚠️ **Serve una policy definitiva prima del deploy**

---

## 8. System Prompt: Regole Chiave

Il system prompt impone:
- uso obbligatorio dei tool
- ordine rigido della pipeline
- uso esclusivo del `normalized_path` per ingestion
- rifiuto esplicito di formati non supportati

Il prompt è considerato **parte dell’architettura**, non testo decorativo.

---

## 9. Git Workflow (Obbligatorio)

- Ogni feature → nuovo branch:

feature/<descrizione>

- Commit frequenti nei punti stabili
- Test manuali via `agentcore invoke --dev`
- Merge in `main` **solo quando stabile**
- Branch eliminato dopo merge

---

## 10. Roadmap Prossimi Step

### A. Excel edge cases
- sheet non specificato
- colonne vuote
- date miste

### B. JSON policy definitiva
- decidere ingestion sì/no
- definire conversione canonica

### C. Configurabilità
- mode = drop_invalid / keep_nulls
- soglia majority rule
- naming table

### D. Ingestion log / metadata table
- tabella Athena non-Iceberg
- traccia file, path, timestamp, esito

### E. Hardening pre-deploy
- error handling
- idempotenza
- documentazione finale

---

## 11. Stato Finale

Il progetto è:
- **strutturalmente solido**
- **concettualmente coerente**
- **pronto per decisioni finali su JSON e deploy**

Questo documento rappresenta **la fonte ufficiale di verità** per riprendere il lavoro in una nuova chat o contesto.