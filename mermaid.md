# Diagram sources (Mermaid)

Source for every diagram in `README.md` and `ARCHITECTURE.md`. The figures follow the
architecture document's Appendix B and have been updated to show what was **built**.
Items marked *(future, Uₙ)* are listed in `UNFINISHED.md`.

Render to `docs/diagrams/*.svg` with:

```bash
python scripts/render_diagrams.py      # uses the minlag/mermaid-cli Docker image
```

Each block is preceded by a `<!-- file: name -->` marker that names its SVG.

## 1. Current implementation (before)

<!-- file: 01-current-implementation -->
```mermaid
flowchart TB
    subgraph HOST["Single process on one machine - app.main.run, invoked by hand"]
        direction LR
        PC["fetch_all_products<br/>all rows held in RAM<br/>serial, one page at a time"] --> CE["export_catalogue<br/>writes CSV"] --> CSV[("catalogue.csv<br/>LOCAL DISK<br/>not S3, not durable")] --> WS["sync_catalogue<br/>re-reads CSV<br/>no validation"] --> WC["send_products<br/>serial batches<br/>one blind retry"] --> OUT["print to stdout<br/>no run status<br/>no metrics, no alerting"]
    end
    subgraph EXT["External systems - outside our ownership"]
        direction LR
        PIM["PIM API<br/>max 10 req/s, 500 per page<br/>latency 100ms to 3s<br/>occasional 429 and 5xx"]
        WMS["WMS API<br/>max 100 per batch, 20 req/s<br/>occasional 429 and 5xx<br/>per-record business rejections"]
    end
    subgraph GAP["Documented flow step 3 is not implemented"]
        direction LR
        S3STUB["storage/s3.py S3Store<br/>writes to the local filesystem<br/>and is never called"]
    end
    HOST <==> EXT
    HOST -.->|"the CSV never reaches S3"| GAP
    classDef bad fill:#fdecea,stroke:#c62828,stroke-width:2px,color:#000
    classDef ext fill:#e8eaf6,stroke:#3949ab,stroke-width:2px,color:#000
    classDef host fill:#fff8e1,stroke:#f9a825,stroke-width:2px,color:#000
    class PIM,WMS ext
    class CSV,OUT,S3STUB bad
    class PC,CE,WS,WC host
```

## 2. Throughput arithmetic and the 30-minute window

<!-- file: 02-throughput -->
```mermaid
flowchart TB
    subgraph TODAY["Serial implementation today - 1.5 s average vendor latency"]
        direction LR
        T1["250k products<br/>500 PIM pages<br/>= 12.5 min"] --> T2["2,500 WMS batches<br/>= 62 min"] --> T3["Total ~75 min<br/>ALREADY MISSES<br/>the 30 min window"]
    end
    subgraph FLOOR["Rate-limit floor at 1M products"]
        direction LR
        R1["PIM at 10 req/s<br/>2,000 pages = 200 s"] --> R2["WMS at 20 req/s<br/>10,000 batches = 500 s"] --> R3["~12 min sequential<br/>~8.5 min overlapped"]
    end
    subgraph DESIGN["What the built design does"]
        direction LR
        C1["in-flight = rate x latency<br/>8 req/s x 3 s = 24 workers"] --> C2["budget 80 % of limits<br/>PIM 8 req/s, WMS 16 req/s<br/>each worker gets budget / N"] --> C3["change detection sends<br/>only changed products<br/>~9 min expected at 1M"]
    end
    TODAY ==> FLOOR ==> DESIGN
    classDef bad fill:#fdecea,stroke:#c62828,color:#000
    classDef ok fill:#e8f5e9,stroke:#2e7d32,color:#000
    classDef warn fill:#fff8e1,stroke:#f9a825,color:#000
    class T3 bad
    class R3,C3 ok
    class C1,C2 warn
```

## 3. Option 1 architecture (as built)

<!-- file: 03-architecture -->
```mermaid
flowchart LR
    EB["EventBridge Scheduler<br/>daily 05:00, retry + DLQ"] --> SFN
    subgraph SFN["Step Functions - Standard workflow"]
        direction TB
        SR["StartRun<br/>run lock, PIM probe,<br/>page batches"] --> MAP["Distributed Map<br/>MaxConcurrency N<br/>ExtractPages"] --> AS["AssembleExport"] --> DM["Map<br/>ValidateAndDiff"] --> WL{"WaitForLoad<br/>CheckLoadProgress"} --> FR["FinalizeRun<br/>reconcile, status,<br/>release lock"]
    end
    PIM["PIM API"] -->|"8 req/s total"| MAP
    MAP --> RAW[("S3 raw<br/>page shards")]
    AS --> EXP[("S3 exports<br/>canonical CSV<br/>90-day lifecycle")]
    AS --> CUR[("S3 curated<br/>work chunks,<br/>quarantine")]
    DM <--> PS[("DynamoDB<br/>product-state<br/>content hashes")]
    DM -->|"changed only,<br/>batches of 100"| Q["SQS load queue"]
    Q --> LB["LoadBatch Lambda<br/>ReportBatchItemFailures<br/>16 req/s total"] --> WMS["WMS API"]
    LB -->|"exhausted, 4xx,<br/>ambiguous"| DLQ["SQS DLQ"]
    LB --> PS
    SR & MAP & DM & LB & FR <--> RUNS[("DynamoDB runs<br/>ledger, pages, chunks,<br/>batches, rejections")]
    SM["Secrets Manager<br/>PIM and WMS keys"] -.-> MAP & LB
    FR -.->|"EMF metrics"| CW["CloudWatch alarms<br/>SNS paging"]
    classDef aws fill:#fff3e0,stroke:#ef6c00,color:#000
    classDef store fill:#e8f5e9,stroke:#2e7d32,color:#000
    classDef ext fill:#e8eaf6,stroke:#3949ab,color:#000
    class EB,SR,MAP,AS,DM,WL,FR,Q,LB,DLQ,SM,CW aws
    class RAW,EXP,CUR,PS,RUNS store
    class PIM,WMS ext
```

## 4. Never sent twice: readings and decision

<!-- file: 04-never-sent-twice -->
```mermaid
flowchart TB
    subgraph READ["Four things 'never sent twice' could mean"]
        direction LR
        I1["I1 - never twice within one run<br/>SKU appears in two batches"]
        I2["I2 - never twice across runs<br/>unchanged products resent"]
        I3["I3 - never APPLIED twice<br/>duplicate delivery is fine<br/>if the WMS deduplicates"]
        I4["I4 - never twice after an<br/>AMBIGUOUS outcome<br/>timeout or reset"]
    end
    subgraph BUILT["How the build satisfies each"]
        direction LR
        B1["dedupe on id in AssembleExport,<br/>one SKU per batch per run"]
        B2["sha256 content hash per SKU,<br/>only changed products queued"]
        B3["derived Idempotency-Key header,<br/>terminal batches never resent<br/>on SQS redelivery"]
        B4["not retried unless<br/>WMS_HONOURS_IDEMPOTENCY_KEY;<br/>parked AMBIGUOUS in the DLQ"]
    end
    subgraph Q["Open question for the Warehouse team"]
        direction LR
        D1["Do you honour Idempotency-Key<br/>on POST /products/batch, and<br/>for how long? Or is there a read<br/>endpoint to confirm a SKU?"]
    end
    I1 --> B1
    I2 --> B2
    I3 --> B3
    I4 --> B4
    BUILT ==> Q
    classDef q fill:#e3f2fd,stroke:#1565c0,color:#000
    classDef ok fill:#e8f5e9,stroke:#2e7d32,color:#000
    classDef warn fill:#fff8e1,stroke:#f9a825,color:#000
    class I1,I2,I3,I4 q
    class B1,B2,B3 ok
    class B4,D1 warn
```

## 5. Request path and outcome classification

<!-- file: 05-request-classification -->
```mermaid
flowchart TB
    subgraph GATE["Before the call leaves the process"]
        direction LR
        A["Outbound call"] --> CB{"Circuit breaker<br/>50 % over 20 calls"} --> RL["Rate limiter<br/>budget / N"] --> CALL["Request<br/>connect 5 s, read 30 s"]
        CB -. OPEN .-> SKIP["CIRCUIT_OPEN<br/>message deferred for<br/>the cooldown, not claimed"]
    end
    subgraph TERM["Terminal outcomes - no retry"]
        direction LR
        OK["2xx - OK<br/>record accepted + rejected,<br/>commit hashes, batch DONE"]
        POISON["400 / 413 - NON_RETRYABLE<br/>straight to DLQ"]
        AUTH["401 / 403 - refresh the<br/>secret once, then AUTH -> DLQ"]
    end
    subgraph RETRY["Retryable and ambiguous outcomes"]
        direction LR
        HON["429 - sleep Retry-After,<br/>reduce the worker's rate"]
        BO["5xx / connect error<br/>full jitter, base 1 s, cap 32 s,<br/>6 attempts per delivery"]
        AMB["read timeout / reset<br/>AMBIGUOUS - never treated as 5xx<br/>(GETs are safe to retry)"]
    end
    GATE ==> TERM
    GATE ==> RETRY
    classDef ok fill:#e8f5e9,stroke:#2e7d32,color:#000
    classDef warn fill:#fff8e1,stroke:#f9a825,color:#000
    classDef err fill:#fdecea,stroke:#c62828,color:#000
    class OK ok
    class BO,HON,RL warn
    class POISON,AUTH,SKIP,AMB err
```

## 6. Load phase and per-outcome handling

<!-- file: 06-load-phase -->
```mermaid
flowchart TB
    subgraph L1["Dispatch - ValidateAndDiff"]
        direction LR
        H(["Changed products"]) --> I["Chunk into batches of 100<br/>batch_id = chunk-position<br/>key = sha256(run_id + skus + hashes)"] --> J["conditional put BATCH PENDING<br/>then SendMessageBatch"]
    end
    subgraph L2["LoadBatch - one SQS message = one WMS batch"]
        direction LR
        G{"Batch status?"} -->|"DONE / DLQ / AMBIGUOUS"| SKIPD["acknowledge,<br/>no WMS call"]
        G -->|"INFLIGHT on redelivery"| AMB1["AMBIGUOUS:<br/>previous outcome unknown"]
        G -->|"PENDING / RETRY"| CLAIM["claim INFLIGHT,<br/>attempts + 1"] --> SEND["POST /products/batch"]
    end
    subgraph L3["Outcome"]
        direction LR
        M["accepted: commit hash<br/>rejected: classify, record,<br/>commit hash -> DONE"]
        O["retryable: RETRY, redelivered;<br/>after 5 delivery attempts -> DLQ"]
        U["timeout: AMBIGUOUS -> DLQ,<br/>hash NOT committed"]
    end
    L1 ==> L2
    SEND ==> L3
    classDef ok fill:#e8f5e9,stroke:#2e7d32,color:#000
    classDef warn fill:#fff8e1,stroke:#f9a825,color:#000
    classDef err fill:#fdecea,stroke:#c62828,color:#000
    class M,SKIPD ok
    class O,CLAIM warn
    class U,AMB1 err
```

## 7. Dead-letter handling and replay

<!-- file: 07-dlq-replay -->
```mermaid
flowchart LR
    subgraph EXH["Into the DLQ - with full context"]
        direction TB
        D1["run_id, batch_id,<br/>idempotency_key, products,<br/>hashes, dlq_reason"]
    end
    subgraph TRIAGE["Triage"]
        direction TB
        T1["Rule-based classifier<br/>SKU ending 999 = expected<br/>missing SKU = defect"] --> T3{"Expected?"}
        T3 -- Yes --> T4["recorded, suppressed,<br/>counted in reconciliation"]
        T3 -- No --> T5["unexpected-rejections alarm"]
        T2["AI-assisted triage<br/>(future, U6)"] -.-> T3
    end
    subgraph REC["Operator-approved recovery"]
        direction TB
        W1["python -m app.main<br/>replay-dlq run_id"] --> W2["DLQ -> RETRY,<br/>same idempotency key,<br/>only failed batches"] --> W3["load drains,<br/>FinalizeRun re-reconciles"]
        W4["--include-ambiguous only after<br/>the WMS team confirms"] -.-> W1
    end
    EXH ==> TRIAGE ==> REC
    classDef ok fill:#e8f5e9,stroke:#2e7d32,color:#000
    classDef warn fill:#fff8e1,stroke:#f9a825,color:#000
    classDef err fill:#fdecea,stroke:#c62828,color:#000
    classDef future fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray: 4 4,color:#555
    class W1,W2,W3,T4 ok
    class T1,W4 warn
    class D1,T5 err
    class T2 future
```

## 8. State model

<!-- file: 08-state-model -->
```mermaid
erDiagram
    SYNC_RUN ||--o{ RUN_PAGE : "extracts"
    SYNC_RUN ||--o{ CHUNK : "diffs"
    SYNC_RUN ||--o{ LOAD_BATCH : "dispatches"
    SYNC_RUN ||--o{ INVALID : "quarantines"
    LOAD_BATCH ||--o{ REJECTION : "produces"
    PRODUCT_STATE ||--o{ LOAD_BATCH : "committed by"
    SYNC_RUN {
        string partition_key "RUN#run_id"
        string sort_key "META"
        string status "RUNNING COMPLETE PARTIAL FAILED NO_CHANGE"
        string export_s3_key "retained CSV"
        number pim_total "total the PIM reported"
        map reconciliation "variances, all zero when healthy"
        string started_at "ISO8601"
        string finished_at "ISO8601"
    }
    RUN_PAGE {
        string sort_key "PAGE#00017"
        string status "OK or DLQ"
        string s3_key "deterministic shard key"
        number rows "products on the page"
        number attempts "HTTP attempts"
    }
    CHUNK {
        string sort_key "CHUNK#00002"
        number rows_invalid "quarantined"
        number rows_unchanged "hash matched"
        number rows_changed "queued"
    }
    LOAD_BATCH {
        string sort_key "BATCH#00002-017"
        string idempotency_key "sha256"
        number batch_size "max 100"
        string status "PENDING INFLIGHT RETRY DONE DLQ AMBIGUOUS"
        number attempts "delivery attempts"
    }
    REJECTION {
        string sort_key "REJECT#SKU#P0000999"
        string reason "verbatim from the WMS"
        string classification "WMS_RULE_SKU_999 MISSING_SKU UNACCOUNTED UNCLASSIFIED"
        boolean expected "true suppresses the alert"
    }
    INVALID {
        string sort_key "INVALID#chunk-row"
        string reason "contract violation"
    }
    PRODUCT_STATE {
        string partition_key "SKU#P0000001"
        string content_hash "sha256 of the WMS payload"
        string last_synced_run "run_id"
        string last_status "ACCEPTED REJECTED"
    }
```

## 9. Terraform layout

<!-- file: 09-terraform -->
```mermaid
flowchart TB
    subgraph ROOT["Root modules - one state per environment"]
        direction LR
        ENVL["envs/local<br/>LocalStack endpoints,<br/>secrets seeded,<br/>enable_compute optional"]
        ENVD["envs/dev<br/>S3 backend + lock"]
        ENVP["envs/prod<br/>S3 backend + lock"]
    end
    subgraph MOD["Reusable modules - no providers, no hardcoded ARNs"]
        direction LR
        M6["security<br/>KMS CMK, Secrets Manager"]
        M4["storage<br/>raw / exports / curated,<br/>90-day lifecycle, versioning,<br/>SSE-KMS + bucket keys"]
        M7["state<br/>runs, product-state,<br/>run-lock tables, PITR"]
        M3["load<br/>SQS + DLQ, loader Lambda,<br/>event source mapping"]
        M2["extract<br/>stage Lambdas,<br/>one role per function"]
        M1["orchestration<br/>Step Functions,<br/>EventBridge Scheduler"]
        M5["observability<br/>alarms, SNS, dashboard"]
    end
    subgraph CI["Pipeline (future)"]
        direction LR
        P1["fmt, validate, tflint,<br/>checkov"] --> P2["plan on PR"] --> P3["apply on merge<br/>via OIDC role"]
    end
    ROOT ==> MOD
    MOD -.-> CI
    classDef aws fill:#fff3e0,stroke:#ef6c00,color:#000
    classDef env fill:#e8eaf6,stroke:#3949ab,color:#000
    classDef future fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray: 4 4,color:#555
    class ENVL,ENVD,ENVP env
    class M1,M2,M3,M4,M5,M6,M7 aws
    class P1,P2,P3 future
```

## 10. Local development and test harness

<!-- file: 10-local-harness -->
```mermaid
flowchart TB
    subgraph COMPOSE["docker compose (docker-compose.yml + docker-compose.localstack.yml)"]
        direction LR
        PIMM["mock PIM :8001<br/>unchanged"]
        WMSM["mock WMS :8002<br/>unchanged"]
        LS["LocalStack :4566<br/>S3, DynamoDB, SQS, KMS,<br/>Secrets, SNS, CloudWatch,<br/>Lambda, Step Functions"]
        TF["terraform container<br/>applies envs/local"]
    end
    subgraph RUN["Ways to run the pipeline"]
        direction LR
        R1["python -m app.main run<br/>local orchestrator, same<br/>stage functions, real SQS"]
        R2["python -m app.main start-execution<br/>the real state machine and<br/>Lambdas inside LocalStack"]
    end
    subgraph TESTS["pytest layers"]
        direction LR
        U["unit + chaos<br/>httpx MockTransport"]
        RC["rate conformance<br/>aggregate rate vs budget"]
        P["pipeline<br/>moto + mocks in-process<br/>+ fault-injecting forwarder"]
        E["e2e (-m e2e)<br/>compose stack, nothing stubbed"]
    end
    TF --> LS
    COMPOSE ==> RUN
    COMPOSE ==> E
    classDef ext fill:#e8eaf6,stroke:#3949ab,color:#000
    classDef ok fill:#e8f5e9,stroke:#2e7d32,color:#000
    classDef aws fill:#fff3e0,stroke:#ef6c00,color:#000
    class PIMM,WMSM ext
    class LS,TF,R1,R2 aws
    class U,RC,P,E ok
```

## 11. Current to target mapping

<!-- file: 11-current-to-target -->
```mermaid
flowchart LR
    subgraph NOW["Before - single serial process"]
        direction TB
        N1["fetch_all_products<br/>whole catalogue in RAM"]
        N2["catalogue.csv on local disk"]
        N3["transform, float price,<br/>no validation"]
        N4["send_products<br/>serial, one retry"]
        N5["print result"]
    end
    subgraph TGT["After - Option 1 (app/)"]
        direction TB
        T1["pipeline/extract.py<br/>clients/product_api.py<br/>resilience/*"]
        T2["pipeline/assemble.py<br/>storage/s3.py - streamed CSV,<br/>90-day lifecycle"]
        T3["domain/mapping.json + models.py<br/>Decimal, quarantine;<br/>pipeline/diff.py hashes"]
        T4["pipeline/load.py<br/>SQS, DLQ, idempotency key,<br/>breaker, partial failure"]
        T5["state/ledger.py, finalize.py<br/>run status, reconciliation,<br/>EMF metrics, alarms"]
    end
    N1 ==>|replace| T1
    N2 ==>|replace| T2
    N3 ==>|harden| T3
    N4 ==>|replace| T4
    N5 ==>|replace| T5
    classDef old fill:#fdecea,stroke:#c62828,color:#000
    classDef new fill:#e8f5e9,stroke:#2e7d32,color:#000
    class N1,N2,N3,N4,N5 old
    class T1,T2,T3,T4,T5 new
```

## 12. Platform decision path

<!-- file: 12-decision-path -->
```mermaid
flowchart TB
    Q0{"Beyond ~2M products, many<br/>sources and destinations, or<br/>freshness tighter than daily?"}
    Q0 -->|Yes| C["Option 3<br/>Lakehouse + streaming"]
    Q0 -->|No| Q1{"A step over 15 minutes,<br/>a static egress IP, or a<br/>hard portability requirement?"}
    Q1 -->|Yes| B["Option 2<br/>ECS Fargate + Airflow"]
    Q1 -->|No| A["Option 1<br/>Serverless event-driven<br/>BUILT"]
    classDef rec fill:#c8e6c9,stroke:#2e7d32,stroke-width:3px,color:#000
    classDef alt fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#000
    class A rec
    class B,C alt
```
