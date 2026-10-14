# Unfinished work and next steps

This is Deliverable 3. Every item is stated openly.

**Built and verified:**
- The Option 1 pipeline, in `app/`.
- Terraform for local, dev and prod. Local was applied to LocalStack, including
  the state machine and Lambdas. Dev and prod pass `terraform validate`.
- 67 unit, chaos, rate-conformance and pipeline tests.
- 8 end-to-end tests against the docker mocks and LocalStack.

**What follows** is what remains, why, and what it would take.

Priorities: **High** means before production; **Medium** means within the
first month; **Low** means when it becomes relevant.

---

## U1 — Deletion / tombstone handling · **High**

| | |
|---|---|
| **Unfinished** | Products withdrawn from the PIM are never removed from the WMS (assumption A10). |
| **Proposed solution** | Compare the SKU set of each complete export with the previous complete run's set (both CSVs are retained). Emit explicit delete instructions for SKUs that disappeared. Guard this with a shrink threshold (for example, refuse if more than 2 % disappear), so a truncated or `PARTIAL` export can never wipe the WMS. Only `export_complete = true` runs are eligible. |
| **Validation** | A test that removes N products between two runs and asserts exactly N deletions. A test that a 50 % shrink is refused and alarmed. A test that a `PARTIAL` run produces no deletions. |
| **Why not done** | The brief never mentions deletions, and the WMS API has no delete endpoint. Guessing delete semantics against a warehouse system is more dangerous than deferring them. |
| **Deliberate or limitation** | Deliberate prioritisation. |
| **Risk** | Withdrawn products stay sellable in the WMS indefinitely. This is the most likely source of a real business incident. |

## U2 — Shared distributed rate limiter · **Low**

| | |
|---|---|
| **Unfinished** | Rate control uses a partitioned static budget (`budget / N`), not a shared token bucket. |
| **Proposed solution** | A DynamoDB (or ElastiCache) token bucket, decremented atomically before each vendor call, so idle workers donate capacity. |
| **Validation** | Extend `tests/test_rate_conformance.py` to several concurrency levels against the shared limiter, asserting that the aggregate rate never exceeds the budget. |
| **Why not done** | The partitioned budget is correct and far simpler. It only costs utilisation, and the window is met regardless. |
| **Deliberate or limitation** | Deliberate. |
| **Risk** | Extract and load run somewhat slower than the vendor limits allow: lost headroom, not lost correctness. Also, a cold Lambda container's limiter starts fresh, so one extra call per new container can briefly exceed `budget/N`. That is mitigated by the 80 % budget. |

## U3 — Load test at 1,000,000 products · **High**

| | |
|---|---|
| **Unfinished** | The 30-minute projection at 1M products (≈9 min expected, ≈19 min worst case) is modelled, not measured. The largest measured run is 12,000 products: 38 s with the local orchestrator, 137 s with the state machine inside LocalStack. |
| **Proposed solution** | Deploy `envs/dev` and point it at a synthetic PIM serving 2,000 pages with latency drawn from 100 ms–3 s and enforced 10 / 20 req/s limits (see U8). Run a full-send and a 5 %-change scenario. Measure stage timings, Lambda memory, DynamoDB throttling, finalise query cost and vendor 429 rate. |
| **Validation** | p95 run duration under 20 minutes over 7 consecutive runs, zero reconciliation variance, and a vendor 429 rate below 1 %. |
| **Why not done** | It needs a deployed AWS environment and a PIM that can serve 1M products. Neither exists in the local harness, and the supplied mock holds 12,000 products. |
| **Deliberate or limitation** | Implementation limitation. |
| **Risk** | An unknown bottleneck could appear at the top of the range, for example the ~70 MB id set in AssembleExport or the finalise query over ~10k batch items. The 20-minute overrun alarm gives 10 minutes' warning, and concurrency can be raised without a redeploy. |

## U4 — Athena reconciliation across runs, and a Parquet curated zone · **Medium**

| | |
|---|---|
| **Unfinished** | Reconciliation runs per run, in FinalizeRun, from the DynamoDB ledger. There is no cross-run Athena view. Curated work chunks are JSONL, not Parquet. |
| **Proposed solution** | Write the curated chunks as Parquet (a pyarrow Lambda layer). Declare Glue tables in `modules/storage`. Add a scheduled Athena query that trends accepted, rejected and variance per run, plus a variance alarm on its output. |
| **Validation** | The Athena totals equal the ledger totals for the last 7 runs. A seeded variance triggers the alarm. |
| **Why not done** | The ledger already carries every count, and a single bad run is already caught and alarmed. Cross-run trend analysis matters in week two, not on day one. pyarrow also adds ~50 MB to the Lambda package. |
| **Deliberate or limitation** | Deliberate. |
| **Risk** | A slow leak across runs is harder to spot. Individual runs are fully reconciled. |

## U5 — Replay state machine and operator runbooks · **Medium**

| | |
|---|---|
| **Unfinished** | DLQ replay is a CLI command (`python -m app.main replay-dlq <run_id> [--include-ambiguous]`), not a second state machine. There is no written runbook. |
| **Proposed solution** | A small Step Functions workflow (`ReplayDlq` → `WaitForLoad` → `FinalizeRun`) that reuses the existing stage functions, started by an operator with an approval step. Runbooks for each alarm in `modules/observability`. |
| **Validation** | A game day: inject a persistent WMS failure for one batch, confirm the DLQ alarm, replay, and assert that only that batch is resent, with its original key. The CLI path is already covered by `test_persistent_wms_failure_dead_letters_only_that_batch_and_replay_recovers_it`. |
| **Why not done** | The DLQ already holds every failed batch with full context, so nothing is lost. Replay works today; it just needs an engineer. |
| **Deliberate or limitation** | Deliberate. |
| **Risk** | Recovery needs an engineer rather than an operator: longer time to recover, no data risk. |

## U6 — AI layer (mapping co-pilot, RAG operations triage) · **Low**

| | |
|---|---|
| **Unfinished** | Nothing from section 15 of the architecture document is built. Rejection triage is rule-based only (`app/domain/rejections.py`). |
| **Proposed solution** | As designed: a design-time mapping and test co-pilot whose output is `mapping.json` changes merged by PR, and a Bedrock knowledge base over the specs, runbooks and incidents to triage rejections and draft remediation. Read-only credentials, guardrails, citations, an evaluation set gating deployment, and a human approval gate on every action. |
| **Validation** | Classify a seeded rejection mix (398 × `999`, 14 × missing SKU) with citations, scored against the evaluation set. |
| **Why not done** | An AI layer over an unproven pipeline would explain failures that shouldn't happen. The sequencing is the point. |
| **Deliberate or limitation** | Deliberate. |
| **Risk** | None to correctness. The cost is operator time on triage. |

## U7 — Secret rotation and VPC endpoints · **Medium**

| | |
|---|---|
| **Unfinished** | The secrets are out of source and KMS-encrypted, but have no rotation schedule. The Lambdas are not VPC-attached, so there are no VPC endpoints. |
| **Proposed solution** | A rotation Lambda per secret, coordinated with the vendor key-issuing process. If a vendor needs a static egress IP, use VPC-attached Lambdas with a NAT gateway (~USD 35/month) plus interface endpoints for S3, DynamoDB, SQS and Secrets Manager. |
| **Validation** | Force a rotation mid-run and assert the run completes. The client already refreshes the secret once on 401/403 (tested in `test_401_refreshes_the_secret_once_then_succeeds`). |
| **Why not done** | The critical part of finding F7 (keys in source) is closed. Rotation needs vendor co-operation. |
| **Deliberate or limitation** | Deliberate. |
| **Risk** | A leaked key stays valid until someone notices. |

## U8 — Fault-injection proxy for latency and rate enforcement · **High**

| | |
|---|---|
| **Unfinished** | The supplied mocks respond instantly, enforce no rate limits and never time out. Because of the `JSONResponse` argument order they also turn their injected 429/503 into plain 500s with no `Retry-After` (see ARCHITECTURE.md section 1). Latency, rate enforcement and Retry-After are therefore proven by unit, chaos and conformance tests with stubbed transports, not by the docker stack. |
| **Proposed solution** | A small proxy container in front of each mock that adds latency drawn from the documented 100 ms–3 s distribution, returns real 429s with `Retry-After` above the documented rates, and can drop a connection after forwarding (a true ambiguous outcome). The mocks stay unmodified. |
| **Validation** | Run the e2e suite through the proxy. Assert that the observed aggregate rate stays at or below 10 / 20 req/s, that 429s are honoured, and that a deliberately naive per-worker limiter fails the same test. |
| **Why not done** | Writing it well is a meaningful piece of work in its own right. The in-process `Forwarder` in `tests/conftest.py` already injects persistent 5xx and timeouts for the pipeline tests. |
| **Deliberate or limitation** | Implementation limitation within the time box. |
| **Risk** | Concurrency and limiter behaviour under real latency are reasoned and unit-tested, but not demonstrated end to end. This is the first gap I would close after U1. |

## U9 — Multi-region disaster recovery · **Low**

| | |
|---|---|
| **Unfinished** | Single region. |
| **Proposed solution** | Cross-region replication of the exports bucket, DynamoDB global tables for `product-state`, and a documented regional failover. The pipeline itself is reproducible from Terraform in any region. |
| **Validation** | Apply `envs/prod` in a second region and run against the replicated state. |
| **Why not done** | There is no availability requirement beyond the daily window, and a missed run can be re-run. |
| **Deliberate or limitation** | Deliberate. |
| **Risk** | A regional outage delays one day's sync. |

## U10 — CI/CD pipeline for Terraform and code · **Medium**

| | |
|---|---|
| **Unfinished** | No pipeline definition. Terraform was formatted and validated, and applied to LocalStack, by hand via the Terraform container. |
| **Proposed solution** | GitHub Actions: `pytest`, `terraform fmt -check`, `validate`, `tflint`, `checkov`, `scripts/build_lambda.py`, plan on PR (posted as a comment), apply on merge through an OIDC-federated role with a manual approval for prod, and scheduled drift detection. |
| **Validation** | A PR with a deliberately public bucket or a wildcard IAM action fails the checks. |
| **Why not done** | Time-boxed in favour of the pipeline's correctness. |
| **Deliberate or limitation** | Deliberate. |
| **Risk** | Changes rely on reviewer discipline rather than enforced gates. |

## U11 — Operator tooling for AMBIGUOUS batches · **Medium**

| | |
|---|---|
| **Unfinished** | An AMBIGUOUS batch is parked in the DLQ, its hash is not committed, and it is alarmed. The operator's decision has to be applied by hand: either replay with `--include-ambiguous`, or accept the batch as applied. There is no "mark as applied" command. A later run will also resend an ambiguous SKU, under a new run's key, because its hash was not committed (as the design document specifies). |
| **Proposed solution** | Two commands: `resolve-ambiguous <run_id> <batch_id> --applied` (commits the hashes and marks the batch DONE) and `--not-applied` (replays it). Once the Warehouse team answers the idempotency-key question, either set `WMS_HONOURS_IDEMPOTENCY_KEY=true` or add a read-back check. |
| **Validation** | Pipeline tests for both resolutions, plus a test that the next run does not resend a batch resolved as applied. |
| **Why not done** | The right behaviour depends on the Warehouse team's answer (section 7 of ARCHITECTURE.md). |
| **Deliberate or limitation** | Deliberate. Waiting on a business answer. |
| **Risk** | An ambiguous batch needs manual handling. It is never silently duplicated. |

## U12 — LocalStack emulation gaps · **Low**

| | |
|---|---|
| **Unfinished** | LocalStack 4.4 cannot parse Distributed Map `ItemBatcher` and only supports STANDARD map children. The ASL uses pre-batched items (equivalent), and `map_execution_type` is EXPRESS in AWS and STANDARD locally. So the EXPRESS configuration has not been executed anywhere. |
| **Proposed solution** | Run the dev environment once in AWS. Or use a LocalStack Pro image, which supports both. |
| **Validation** | A successful EXPRESS-child execution in dev. |
| **Why not done** | No AWS account in scope for the exercise. |
| **Deliberate or limitation** | Implementation limitation (emulator). |
| **Risk** | Low. The difference is the child execution type only; the item processor is identical. |
