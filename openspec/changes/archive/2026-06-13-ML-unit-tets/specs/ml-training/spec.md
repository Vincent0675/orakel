# Delta for ml-training

## Overview

The existing `ml-training` specification (ML-1 through ML-10) is fully validated by the ML unit tests. No behavior changes are introduced — the tests verify that the existing requirements are correctly implemented.

## MODIFIED Requirements

### Requirement: ML-1 through ML-10 (No behavior change)

(Previously: standalone spec with no test validation. Now: all requirements are exercised by ML unit tests.)

The requirements ML-1 through ML-10 remain unchanged. The following test coverage validates each requirement:

| Requirement | Validated By | Phase |
|-------------|-------------|-------|
| ML-1 (Feature engineering) | MLT-4 scenarios: role counts, affix flags, log1p, NULL handling | Phase 2 |
| ML-2 (Numeric features) | MLT-4 scenario: NULL KPI columns imputed | Phase 2 |
| ML-3 (Ridge model) | MLT-2 scenario: train with synthetic DataFrame | Phase 1 |
| ML-4 (Temporal split) | MLT-2 scenario: temporal split respects chronological order | Phase 1 |
| ML-5 (Target column) | MLT-1 scenario: target column is defined | Phase 1 |
| ML-6 (Evaluation metrics) | MLT-2 scenario: MLflow logs parameters and metrics | Phase 1 |
| ML-7 (Baseline comparison) | MLT-2 scenario: metrics dict contains baseline_mae | Phase 1 |
| ML-8 (MLflow tracking) | MLT-2 scenario: MLflow logs parameters and metrics | Phase 1 |
| ML-9 (Feature importance) | MLT-2 scenario: MinIO upload is called (pipeline includes importance CSV) | Phase 1 |
| ML-10 (Model serialization) | MLT-2 scenario: MinIO upload is called | Phase 1 |

#### Scenario: All existing ML behaviors remain valid

- GIVEN the existing `ml-training` spec (ML-1 through ML-10)
- WHEN the ML unit tests from Phase 1 and Phase 2 pass
- THEN all existing requirements SHALL be validated by at least one test
- AND no spec-level behavior changes SHALL be introduced
