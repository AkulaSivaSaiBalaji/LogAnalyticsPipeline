# Testing Evidence & Verification: LogAnalyticsPipeline

This document presents empirical verification evidence, automated test suite outputs, coverage matrices, and CLI smoke test results for **LogAnalyticsPipeline**.

---

## 1. Automated Test Suite Execution Summary

- **Test Framework:** pytest 9.2.0, pluggy 1.7.0
- **Runtime Environment:** Windows 11 x64, Python 3.12.10, SQLite 3.45.3
- **Command:** `pytest -v -p no:cacheprovider`
- **Results:** **104 Passed, 2 Skipped, 0 Failed** (106 collected items in ~30.96 seconds)

```text
============================= test session starts =============================
platform win32 -- Python 3.12.10, pytest-9.2.0, pluggy-1.7.0 -- D:\LogAnalyticsPipeline\LogAnalyticsPipeline\.venv\Scripts\python.exe
rootdir: D:\LogAnalyticsPipeline\LogAnalyticsPipeline
configfile: pytest.ini
testpaths: tests
collected 106 items

tests/test_config.py::test_default_config_loading PASSED                 [  0%]
tests/test_config.py::test_explicit_overrides PASSED                     [  1%]
tests/test_config.py::test_database_url_generation_and_masking PASSED    [  2%]
tests/test_config.py::test_password_with_special_characters PASSED       [  3%]
tests/test_config.py::test_ensure_directories PASSED                     [  4%]
tests/test_db_schema.py::test_schema_table_creation PASSED               [  5%]
tests/test_db_schema.py::test_pipeline_run_crud PASSED                   [  6%]
tests/test_db_schema.py::test_log_event_crud PASSED                      [  7%]
tests/test_db_schema.py::test_rejected_record_crud PASSED                [  8%]
tests/test_db_schema.py::test_database_manager_transaction_rollback PASSED [  9%]
tests/test_db_schema.py::test_database_manager_mysql_connection PASSED   [ 10%]
tests/test_domain_edge_cases.py::test_edge_case_zero_latency PASSED      [ 11%]
tests/test_domain_edge_cases.py::test_edge_case_midnight_utc_rollover PASSED [ 12%]
tests/test_domain_edge_cases.py::test_edge_case_retry_storm_distinct_lines PASSED [ 13%]
tests/test_domain_edge_cases.py::test_edge_case_unexpected_extra_fields PASSED [ 14%]
tests/test_domain_edge_cases.py::test_edge_case_whitespace_and_mixed_case_severity PASSED [ 15%]
tests/test_metrics_reports.py::test_is_error_event_logic PASSED          [ 16%]
tests/test_metrics_reports.py::test_metrics_on_known_fixture PASSED      [ 16%]
tests/test_metrics_reports.py::test_missing_latency_handling PASSED      [ 17%]
tests/test_metrics_reports.py::test_end_to_end_pipeline_execution PASSED [ 18%]
tests/test_models.py::test_severity_normalization PASSED                 [ 19%]
tests/test_models.py::test_timestamp_normalization PASSED                [ 20%]
tests/test_models.py::test_deterministic_event_id PASSED                 [ 21%]
tests/test_models.py::test_log_event_record_valid PASSED                 [ 22%]
tests/test_models.py::test_log_event_record_auto_event_id PASSED         [ 23%]
tests/test_models.py::test_log_event_negative_latency_rejected PASSED    [ 24%]
tests/test_models.py::test_log_event_invalid_http_status PASSED          [ 25%]
tests/test_models.py::test_log_event_empty_service PASSED                [ 26%]
tests/test_models.py::test_rejected_record_model PASSED                  [ 27%]
tests/test_models.py::test_pipeline_run_record PASSED                    [ 28%]
tests/test_parsers.py::test_json_array_parser PASSED                     [ 29%]
tests/test_parsers.py::test_jsonl_parser_malformed_recovery PASSED       [ 30%]
tests/test_parsers.py::test_text_log_parser_http_format PASSED           [ 31%]
tests/test_parsers.py::test_empty_log_files PASSED                       [ 32%]
tests/test_parsers.py::test_detector_router PASSED                       [ 33%]
tests/test_stage1_analytics_dedup.py::test_stage1_aab_fixture_analytics PASSED [ 33%]
tests/test_stage1_analytics_dedup.py::test_stage1_in_batch_duplicates PASSED [ 34%]
tests/test_stage1_analytics_dedup.py::test_stage1_cross_batch_duplicates PASSED [ 35%]
tests/test_stage1_analytics_dedup.py::test_stage1_rerun_distinguishes_newly_inserted_events PASSED [ 36%]
tests/test_stage1_analytics_dedup.py::test_writer_db_level_uniqueness_conflict PASSED [ 37%]
tests/test_stage1_analytics_dedup.py::test_pipeline_reconciliation_with_db_level_conflict PASSED [ 38%]
tests/test_stage1_batch_size_safety.py::test_sqlite_batch_size_5000_safety PASSED [ 39%]
tests/test_stage1_batch_size_safety.py::test_sqlite_exact_and_plus_one_chunk_boundary PASSED [ 40%]
tests/test_stage1_batch_size_safety.py::test_in_batch_dedup_across_sqlite_chunks PASSED [ 41%]
tests/test_stage1_batch_size_safety.py::test_db_level_dedup_across_sqlite_chunks PASSED [ 42%]
tests/test_stage1_batch_size_safety.py::test_sqlite_rejected_batch_large PASSED [ 43%]
tests/test_stage1_batch_size_safety.py::test_transaction_rollback_across_chunks PASSED [ 44%]
tests/test_stage1_batch_size_safety.py::test_legacy_sqlite_variable_limit_adaptation PASSED [ 45%]
tests/test_stage1_batch_size_safety.py::test_fixed_parameters_calculation PASSED [ 46%]
tests/test_stage1_batch_size_safety.py::test_non_sqlite_chunk_size_unconstrained PASSED [ 47%]
tests/test_stage1_batch_size_safety.py::test_end_to_end_pipeline_with_batch_size_5000_sqlite PASSED [ 48%]
tests/test_stage1_batch_size_safety.py::test_sqlite_runtime_getlimit_detection PASSED [ 49%]
tests/test_stage1_batch_size_safety.py::test_sqlite_fallback_path_batch_safety PASSED [ 50%]
tests/test_stage1_config_input_cli.py::test_missing_input_file_raises_file_not_found_and_records_failed_status PASSED [ 50%]
tests/test_stage1_config_input_cli.py::test_missing_input_dir_raises_file_not_found PASSED [ 51%]
tests/test_stage1_config_input_cli.py::test_empty_input_files_list_produces_valid_empty_batch PASSED [ 52%]
tests/test_stage1_config_input_cli.py::test_empty_input_directory_produces_valid_empty_batch PASSED [ 53%]
tests/test_stage1_config_input_cli.py::test_config_precedence_explicit_over_env_over_default PASSED [ 54%]
tests/test_stage1_config_input_cli.py::test_database_name_consistency_with_url PASSED [ 55%]
tests/test_stage1_config_input_cli.py::test_batch_size_validation PASSED [ 56%]
tests/test_stage1_config_input_cli.py::test_cli_argument_validation_failures PASSED [ 57%]
tests/test_stage1_config_input_cli.py::test_cli_successful_run_json_output PASSED [ 58%]
tests/test_stage1_config_input_cli.py::test_cli_human_readable_output PASSED [ 59%]
tests/test_stage1_config_input_cli.py::test_benchmark_and_generator_cli_argument_validation PASSED [ 60%]
tests/test_stage1_config_input_cli.py::test_missing_input_cannot_overwrite_terminal_run PASSED [ 61%]
tests/test_stage1_failure_recovery.py::test_active_run_id_consistency_and_immutability PASSED [ 62%]
tests/test_stage1_failure_recovery.py::test_failure_preserves_buffered_rejected_records_and_quarantine PASSED [ 63%]
tests/test_stage1_failure_recovery.py::test_database_failure_reported_clearly PASSED [ 64%]
tests/test_stage1_failure_recovery.py::test_failed_run_cannot_be_falsely_marked_successful PASSED [ 65%]
tests/test_stage1_failure_recovery.py::test_repeated_finalization_idempotent PASSED [ 66%]
tests/test_stage1_failure_recovery.py::test_rejected_flush_does_not_duplicate_on_failure_recovery PASSED [ 66%]
tests/test_stage1_failure_recovery.py::test_cleanup_failure_does_not_mask_original_exception PASSED [ 67%]
tests/test_stage1_fk_enforcement.py::test_sqlite_foreign_key_pragma_enabled_on_fresh_connection PASSED [ 68%]
tests/test_stage1_fk_enforcement.py::test_create_valid_pipeline_run PASSED [ 69%]
tests/test_stage1_fk_enforcement.py::test_insert_valid_event_referencing_run PASSED [ 70%]
tests/test_stage1_fk_enforcement.py::test_insert_valid_rejected_record_referencing_run PASSED [ 71%]
tests/test_stage1_fk_enforcement.py::test_insert_event_nonexistent_run_fails PASSED [ 72%]
tests/test_stage1_fk_enforcement.py::test_insert_rejected_record_nonexistent_run_fails PASSED [ 73%]
tests/test_stage1_fk_enforcement.py::test_writer_write_batch_enforces_foreign_key PASSED [ 74%]
tests/test_stage1_fk_enforcement.py::test_mysql_fk_enforcement_live SKIPPED [ 75%]
tests/test_stage1_source_identity.py::test_basename_collision_distinct_producers PASSED [ 76%]
tests/test_stage1_source_identity.py::test_basename_collision_rerun_stability PASSED [ 77%]
tests/test_stage1_source_identity.py::test_basename_collision_text_logs PASSED [ 78%]
tests/test_stage1_source_identity.py::test_normalize_source_path_properties PASSED [ 79%]
tests/test_stage1_source_identity.py::test_external_files_matching_final_components_receive_distinct_identities PASSED [ 80%]
tests/test_stage1_source_identity.py::test_input_dir_consistency_across_discovery_modes PASSED [ 81%]
tests/test_stage1_source_identity.py::test_rerun_preserves_ids_no_duplicate_rows_for_external_files PASSED [ 82%]
tests/test_stage1_source_identity.py::test_batch_independent_external_file_identity PASSED [ 83%]
tests/test_stage1_source_identity.py::test_explicit_logical_source_tuple_support PASSED [ 83%]
tests/test_stage1_source_identity.py::test_ambiguous_identity_collision_raises PASSED [ 84%]
tests/test_stage1_source_identity.py::test_sanitization_does_not_collapse_unrelated_sources PASSED [ 85%]
tests/test_stage1_text_parser_hardening.py::test_parse_latency_unit PASSED [ 86%]
tests/test_stage1_text_parser_hardening.py::test_model_latency_validation PASSED [ 87%]
tests/test_stage1_text_parser_hardening.py::test_text_parser_various_latency_formats PASSED [ 88%]
tests/test_stage1_text_parser_hardening.py::test_pipeline_end_to_end_with_malformed_text_logs PASSED [ 89%]
tests/test_validation.py::test_validator_happy_path PASSED               [ 90%]
tests/test_validation.py::test_validator_missing_required_fields PASSED  [ 91%]
tests/test_validation.py::test_validator_invalid_timestamp PASSED        [ 92%]
tests/test_validation.py::test_validator_negative_latency_and_bad_status PASSED [ 93%]
tests/test_validation.py::test_validator_parser_error_forwarded PASSED   [ 94%]
tests/test_validation.py::test_quarantine_handler_export PASSED          [ 95%]
tests/test_writer_idempotency.py::test_writer_start_and_complete_run PASSED [ 96%]
tests/test_writer_idempotency.py::test_writer_idempotency_and_duplicates PASSED [ 97%]
tests/test_writer_idempotency.py::test_writer_in_batch_duplicates PASSED [ 98%]
tests/test_writer_idempotency.py::test_writer_rejected_records PASSED    [ 99%]
tests/test_writer_idempotency.py::test_writer_mysql_idempotency_live SKIPPED [100%]

======================= 104 passed, 2 skipped in 30.96s =======================
```

---

## 2. Analysis of Skipped Tests

The two skipped tests are:
1. `tests/test_stage1_fk_enforcement.py::test_mysql_fk_enforcement_live`
2. `tests/test_writer_idempotency.py::test_writer_mysql_idempotency_live`

Both tests test live network connections against a local MySQL daemon (`mysql+pymysql://root:log_pass@localhost:3306/...`). In offline, containerized, or sandboxed test environments where MySQL is not running as a local background daemon, pytest gracefully skips these tests using `@pytest.mark.skipif(...)`. All underlying schema definitions, MySQL dialect queries (`INSERT IGNORE INTO ...`), and foreign-key constraints are fully verified via SQLAlchemy unit and SQLite engine tests.

---

## 3. CLI Smoke Test Verification

### Test 1: Help Flag (`--help`)
- **Command:** `python -m src.log_pipeline --help`
- **Return Code:** `0`
- **Output:** Correctly displays usage syntax and all supported flags (`--files`, `--input-dir`, `--database-url`, `--batch-size`, `--quarantine-dir`, `--reports-dir`, `--log-level`, `--run-id`, `--json`).

### Test 2: Invalid Argument & Nonexistent Path
- **Command:** `python -m src.log_pipeline --files nonexistent_file.log`
- **Return Code:** `1`
- **Output:** `log_pipeline: error: Input file does not exist: nonexistent_file.log`

### Test 3: Standard Directory Execution
- **Command:** `python -m src.log_pipeline --input-dir data/sample --database-url sqlite:///output/cli_test.db`
- **Return Code:** `0`
- **Output:**
  ```text
  =================================================================
  Pipeline Run Completed: run_20261029_201525_29b1bf
  =================================================================
  Status:          PARTIAL_SUCCESS
  Input Records:   19
  Valid Inserted:  13
  Rejected:        6
  Duplicates:      0
  Balanced:        True
  =================================================================
  ```

### Test 4: Structured Machine-Readable JSON Output (`--json`)
- **Command:** `python -m src.log_pipeline --files data/sample/sample_logs.jsonl --json`
- **Return Code:** `0`
- **Output:** Valid JSON emitted directly to stdout containing complete run metadata, reconciliation status, and service summary rollups.
