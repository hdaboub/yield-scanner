import datetime as dt
import json
import sqlite3
import statistics
import tempfile
import unittest
from pathlib import Path
from collections import defaultdict
from unittest.mock import patch

import scanner
from scanner import Observation, SourceConfig, normalize_row, rank_pools, resolve_endpoint


class AnalyticsTests(unittest.TestCase):
    def test_choose_llama_adaptive_thresholds(self) -> None:
        t_small = scanner.choose_llama_adaptive_thresholds(
            total_rows=5_000,
            min_swap_count_floor=10,
            min_weth_liquidity_floor=50.0,
            baseline_hours=72,
            persistence_hours=6,
            persistence_spike_multiplier=3.0,
            persistence_min_hits=2,
            band="default",
        )
        self.assertEqual(t_small.min_swap_count, 10)
        self.assertAlmostEqual(t_small.min_weth_liquidity, 50.0)

        t_large = scanner.choose_llama_adaptive_thresholds(
            total_rows=200_000,
            min_swap_count_floor=20,
            min_weth_liquidity_floor=80.0,
            baseline_hours=72,
            persistence_hours=6,
            persistence_spike_multiplier=3.0,
            persistence_min_hits=2,
            band="strict",
        )
        self.assertEqual(t_large.min_swap_count, 20)
        self.assertAlmostEqual(t_large.min_weth_liquidity, 80.0)

    def test_build_llama_spike_rankings_uses_baseline_and_persistence(self) -> None:
        base_ts = int(dt.datetime(2026, 2, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[scanner.LlamaPairHourRow] = []
        for hour in range(10):
            ts = base_ts + hour * 3600
            score = 0.001
            if hour in {8, 9}:
                score = 0.005
            rows.append(
                scanner.LlamaPairHourRow(
                    source_name="sushi-v2-fee-spikes-mainnet",
                    version="v2",
                    chain="ethereum",
                    endpoint="https://example.invalid",
                    hour_start_unix=ts,
                    hour_start_utc=scanner.iso_hour(ts),
                    hour_start_chicago=scanner.iso_hour_chicago(ts),
                    pair="0xpair",
                    token0="0xweth",
                    token1="0xtoken",
                    token0_symbol="WETH",
                    token1_symbol="TOK",
                    token0_decimals=18,
                    token1_decimals=18,
                    swap_count=20,
                    fee0_raw="1",
                    fee1_raw="0",
                    reserve0_raw="1000",
                    reserve1_raw="2000",
                    fee0=1.0,
                    fee1=0.0,
                    reserve0=1000.0,
                    reserve1=2000.0,
                    weth_fee=score * 1000.0,
                    weth_reserve=1000.0,
                    weth_fee_normalized=score * 1000.0,
                    weth_reserve_normalized=1000.0,
                    score=score,
                )
            )
        thresholds = scanner.choose_llama_adaptive_thresholds(
            total_rows=len(rows),
            min_swap_count_floor=0,
            min_weth_liquidity_floor=0.0,
            baseline_hours=72,
            persistence_hours=6,
            persistence_spike_multiplier=3.0,
            persistence_min_hits=2,
        )
        ranked, counters = scanner.build_llama_spike_rankings(
            rows,
            thresholds,
            min_baseline_observations=3,
            baseline_epsilon=1e-10,
            spike_multiplier_cap=250.0,
        )
        self.assertTrue(ranked)
        top = ranked[0]
        self.assertGreaterEqual(top.spike_multiplier, 3.0)
        self.assertGreaterEqual(top.persistence_hits, 2)
        self.assertEqual(top.pair, "0xpair")
        self.assertGreater(counters.fetched_raw_rows, 0)

    def test_llama_fallback_relaxes_when_sparse(self) -> None:
        base_ts = int(dt.datetime(2026, 2, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[scanner.LlamaPairHourRow] = []
        for hour in range(8):
            ts = base_ts + hour * 3600
            rows.append(
                scanner.LlamaPairHourRow(
                    source_name="sushi-v2-fee-spikes-mainnet",
                    version="v2",
                    chain="ethereum",
                    endpoint="https://example.invalid",
                    hour_start_unix=ts,
                    hour_start_utc=scanner.iso_hour(ts),
                    hour_start_chicago=scanner.iso_hour_chicago(ts),
                    pair="0xpair",
                    token0="0xweth",
                    token1="0xtoken",
                    token0_symbol="WETH",
                    token1_symbol="TOK",
                    token0_decimals=18,
                    token1_decimals=18,
                    swap_count=6,
                    fee0_raw="1",
                    fee1_raw="0",
                    reserve0_raw="30",
                    reserve1_raw="2000",
                    fee0=1.0,
                    fee1=0.0,
                    reserve0=30.0,
                    reserve1=2000.0,
                    weth_fee=0.12,
                    weth_reserve=30.0,
                    weth_fee_normalized=0.12,
                    weth_reserve_normalized=30.0,
                    score=0.004,
                )
            )
        ranked, thresholds, _counters = scanner.build_llama_rankings_with_fallback(
            rows,
            baseline_hours=72,
            persistence_hours=6,
            persistence_spike_multiplier=1.0,
            persistence_min_hits=1,
            min_baseline_observations=3,
            baseline_epsilon=1e-10,
            spike_multiplier_cap=250.0,
            strict_mode=True,
            default_min_swap_count=10,
            default_min_weth_liquidity=50.0,
            strict_min_swap_count=30,
            strict_min_weth_liquidity=100.0,
            min_ranked_target=1,
        )
        self.assertTrue(ranked)
        self.assertEqual(thresholds.min_swap_count, 5)
        self.assertAlmostEqual(thresholds.min_weth_liquidity, 25.0)

    def test_build_llama_schedule_from_rankings_produces_blocks(self) -> None:
        base = int(dt.datetime(2026, 2, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[scanner.LlamaSpikeRankingRow] = []
        for w in range(3):
            ts = base + (w * 7 * 24 + 12) * 3600  # recurring Monday 12:00 UTC bucket
            rows.append(
                scanner.LlamaSpikeRankingRow(
                    source_name="sushi-v2-fee-spikes-mainnet",
                    chain="ethereum",
                    hour_start_unix=ts,
                    hour_start_utc=scanner.iso_hour(ts),
                    hour_start_chicago=scanner.iso_hour_chicago(ts),
                    pair="0xpair",
                    token0="0xweth",
                    token1="0xtok",
                    token0_symbol="WETH",
                    token1_symbol="TOK",
                    swap_count=15,
                    weth_reserve=100.0,
                    weth_fee=0.2,
                    weth_reserve_normalized=100.0,
                    weth_fee_normalized=0.2,
                    score=0.002,
                    hourly_yield_pct=0.2,
                    usd_per_1000_liquidity_hourly=2.0,
                    rough_apr_pct=1752.0,
                    baseline_median_score=0.0005,
                    spike_multiplier=4.0,
                    spike_multiplier_capped=4.0,
                    persistence_hits=2,
                    notes_flags="",
                )
            )
        schedules = scanner.build_llama_schedule_from_rankings(
            llama_rows=rows,
            end_ts=base + 30 * 24 * 3600,
            top_pairs=10,
            min_occurrences=2,
            min_hit_rate=0.5,
            max_blocks_per_pool=3,
        )
        self.assertTrue(schedules)
        self.assertEqual(schedules[0].source_name, "sushi-v2-fee-spikes-mainnet")

    def test_llama_diagnostics_messages_for_empty_cases(self) -> None:
        counts_a = scanner.LlamaDropoffCounters(
            fetched_raw_rows=0,
            after_time_window_filter=0,
            after_min_swaps_filter=0,
            after_min_weth_filter=0,
            after_baseline_ready_filter=0,
            after_spike_multiplier_filter=0,
            after_persistence_filter=0,
            final_ranked_rows=0,
        )
        stage_a, msg_a = scanner.summarize_llama_dropoff(counts_a)
        self.assertEqual(stage_a, "fetched_raw_rows")
        self.assertIn("0 fetched", msg_a)

        counts_b = scanner.LlamaDropoffCounters(
            fetched_raw_rows=50,
            after_time_window_filter=50,
            after_min_swaps_filter=22,
            after_min_weth_filter=0,
            after_baseline_ready_filter=0,
            after_spike_multiplier_filter=0,
            after_persistence_filter=0,
            final_ranked_rows=0,
        )
        stage_b, msg_b = scanner.summarize_llama_dropoff(counts_b)
        self.assertEqual(stage_b, "after_min_weth_filter")
        self.assertIn("filtered out", msg_b)
        self.assertIn("after_min_swaps_filter=22", msg_b)

    def test_report_llama_empty_case_a_shows_zero_fetched(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "report.html"
            diagnostics = scanner.LlamaRunDiagnostics(
                endpoint="https://api.studio.thegraph.com/query/1742316/llama/v0.2.9",
                version="v2",
                source_name="sushi-v2-fee-spikes-mainnet",
                window_start_ts=1_700_000_000,
                window_end_ts=1_700_086_400,
                local_timezone="America/Chicago",
                thresholds_label="default",
                min_swap_count=10,
                min_weth_liquidity=50.0,
                baseline_hours=72,
                persistence_hours=6,
                persistence_spike_multiplier=3.0,
                persistence_min_hits=2,
                fallback_trace="50/10 -> 25/5 -> 10/3",
                counts=scanner.LlamaDropoffCounters(0, 0, 0, 0, 0, 0, 0, 0),
                meta_block_number=12345,
                seed_next_index=4457,
                seed_total=4457,
                seed_last_block=22_000_000,
                empty_stage="fetched_raw_rows",
                empty_message="No PairHourData rows returned from llama for the queried window (0 fetched).",
            )
            scanner.write_report_html(
                path=out,
                rankings=[],
                schedules=[],
                schedule_enhanced=[],
                spike_run_stats=[],
                moves_day_curve=[],
                schedule_run_diagnostics=[],
                v2_spike_rows=[],
                llama_rows=[],
                llama_thresholds=None,
                llama_sources=[
                    scanner.SourceConfig(
                        name="sushi-v2-fee-spikes-mainnet",
                        version="v2",
                        chain="ethereum",
                        endpoint=diagnostics.endpoint,
                        hourly_query="query {}",
                        source_type="v2_spike",
                    )
                ],
                llama_diagnostics=diagnostics,
                v2_spike_top=25,
                top_n=10,
                start_ts=diagnostics.window_start_ts,
                end_ts=diagnostics.window_end_ts,
                source_count=1,
                local_timezone="America/Chicago",
                min_tvl_usd=0.0,
                max_hourly_yield_pct=None,
                quality_input_rows=0,
                quality_output_rows=0,
                quality_rejected_rows=0,
            )
            text = out.read_text(encoding="utf-8")
            self.assertIn("0 fetched", text)
            self.assertIn("meta.block=12345", text)
            self.assertIn("Window (CST)", text)

    def test_report_llama_empty_case_b_shows_filtered_out_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "report.html"
            diagnostics = scanner.LlamaRunDiagnostics(
                endpoint="https://api.studio.thegraph.com/query/1742316/llama/v0.2.9",
                version="v2",
                source_name="sushi-v2-fee-spikes-mainnet",
                window_start_ts=1_700_000_000,
                window_end_ts=1_700_086_400,
                local_timezone="America/Chicago",
                thresholds_label="default",
                min_swap_count=10,
                min_weth_liquidity=50.0,
                baseline_hours=72,
                persistence_hours=6,
                persistence_spike_multiplier=3.0,
                persistence_min_hits=2,
                fallback_trace="50/10 -> 25/5 -> 10/3",
                counts=scanner.LlamaDropoffCounters(50, 50, 22, 0, 0, 0, 0, 0),
                meta_block_number=12345,
                seed_next_index=4457,
                seed_total=4457,
                seed_last_block=22_000_000,
                empty_stage="after_min_weth_filter",
                empty_message=(
                    "Rows fetched from llama but all were filtered out "
                    "(drop to 0 at after_min_weth_filter after after_min_swaps_filter=22)."
                ),
            )
            scanner.write_report_html(
                path=out,
                rankings=[],
                schedules=[],
                schedule_enhanced=[],
                spike_run_stats=[],
                moves_day_curve=[],
                schedule_run_diagnostics=[],
                v2_spike_rows=[],
                llama_rows=[],
                llama_thresholds=scanner.choose_llama_adaptive_thresholds(
                    total_rows=50,
                    min_swap_count_floor=10,
                    min_weth_liquidity_floor=50.0,
                    baseline_hours=72,
                    persistence_hours=6,
                    persistence_spike_multiplier=3.0,
                    persistence_min_hits=2,
                    band="default",
                ),
                llama_sources=[
                    scanner.SourceConfig(
                        name="sushi-v2-fee-spikes-mainnet",
                        version="v2",
                        chain="ethereum",
                        endpoint=diagnostics.endpoint,
                        hourly_query="query {}",
                        source_type="v2_spike",
                    )
                ],
                llama_diagnostics=diagnostics,
                v2_spike_top=25,
                top_n=10,
                start_ts=diagnostics.window_start_ts,
                end_ts=diagnostics.window_end_ts,
                source_count=1,
                local_timezone="America/Chicago",
                min_tvl_usd=0.0,
                max_hourly_yield_pct=None,
                quality_input_rows=0,
                quality_output_rows=0,
                quality_rejected_rows=0,
            )
            text = out.read_text(encoding="utf-8")
            self.assertIn("filtered out", text)
            self.assertIn("after_min_weth_filter", text)
            self.assertIn("after_min_swaps=22", text)

    def test_spike_run_stats_computes_run_lengths(self) -> None:
        start = int(dt.datetime(2026, 2, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[scanner.Observation] = []
        for hour in range(10):
            ts = start + hour * 3600
            y = 0.0005
            if hour in {2, 3, 4, 7}:
                y = 0.002
            rows.append(
                scanner.Observation(
                    source_name="s1",
                    version="v3",
                    chain="ethereum",
                    pool_id="pool-1",
                    pair="AAA/BBB",
                    fee_tier=3000,
                    ts=ts,
                    volume_usd=100_000.0,
                    tvl_usd=1_000_000.0,
                    fees_usd=y * 1_000_000.0,
                    hourly_yield=y,
                )
            )
        stats = scanner.compute_spike_run_stats(
            observations=rows,
            schedules=[],
            end_ts=start + 10 * 3600,
            window_days=30,
            global_min_threshold=1.5,
            min_nonzero_hours=1,
        )
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0].spike_hours, 4)
        self.assertEqual(stats[0].runs_total, 2)
        self.assertGreaterEqual(stats[0].run_length_p90, 2.0)
        self.assertGreater(stats[0].spike_threshold_usd_per_1000_hr, 0.0)

    def test_spike_run_stats_all_zero_metric_not_always_spike(self) -> None:
        start = int(dt.datetime(2026, 2, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[scanner.Observation] = []
        for hour in range(48):
            ts = start + hour * 3600
            rows.append(
                scanner.Observation(
                    source_name="s1",
                    version="v3",
                    chain="ethereum",
                    pool_id="pool-zero",
                    pair="ZERO/USDC",
                    fee_tier=3000,
                    ts=ts,
                    volume_usd=100_000.0,
                    tvl_usd=1_000_000.0,
                    fees_usd=0.0,
                    hourly_yield=0.0,
                )
            )
        stats = scanner.compute_spike_run_stats(
            observations=rows,
            schedules=[],
            end_ts=start + 48 * 3600,
            window_days=30,
        )
        self.assertEqual(len(stats), 1)
        row = stats[0]
        self.assertGreater(row.spike_threshold_usd_per_1000_hr, 0.0)
        self.assertEqual(row.spike_hours, 0)
        self.assertEqual(row.hit_rate_pct, 0.0)
        self.assertEqual(row.history_quality, "insufficient_nonzero_history")

    def test_spike_run_stats_threshold_never_zero(self) -> None:
        start = int(dt.datetime(2026, 2, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[scanner.Observation] = []
        for hour in range(100):
            ts = start + hour * 3600
            y = 0.0 if hour % 2 == 0 else 0.0003
            rows.append(
                scanner.Observation(
                    source_name="s1",
                    version="v3",
                    chain="base",
                    pool_id="pool-mixed",
                    pair="AAA/BBB",
                    fee_tier=3000,
                    ts=ts,
                    volume_usd=100_000.0,
                    tvl_usd=1_000_000.0,
                    fees_usd=y * 1_000_000.0,
                    hourly_yield=y,
                )
            )
        stats = scanner.compute_spike_run_stats(
            observations=rows,
            schedules=[],
            end_ts=start + 100 * 3600,
            window_days=30,
        )
        self.assertTrue(stats)
        self.assertTrue(all(row.spike_threshold_usd_per_1000_hr > 0.0 for row in stats))

    def test_schedule_enhanced_breakeven_is_nonnegative(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        schedules = [
            scanner.ScheduleRecommendation(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="AAA/BBB",
                fee_tier=3000,
                reliability_hit_rate_pct=70.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                block_hours=2,
                add_day_utc="Monday",
                add_hour_utc=10,
                remove_day_utc="Monday",
                remove_hour_utc=12,
                add_pattern_utc="Monday 10:00 UTC",
                remove_pattern_utc="Monday 12:00 UTC",
                next_add_ts=now + 3600,
                next_remove_ts=now + 10800,
                pool_score=1.0,
            )
        ]
        observations = [
            scanner.Observation(
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="AAA/BBB",
                fee_tier=3000,
                ts=now - (i * 3600),
                volume_usd=100_000.0,
                tvl_usd=1_000_000.0,
                fees_usd=100.0,
                hourly_yield=0.0001,
            )
            for i in range(48)
        ]
        enhanced = scanner.build_schedule_enhanced_rows(
            schedules=schedules,
            observations=observations,
            rankings=[],
            spike_stats=[],
            end_ts=now,
            baseline_window_days=30,
            baseline_top_k=200,
        )
        self.assertEqual(len(enhanced), 1)
        self.assertGreaterEqual(enhanced[0].breakeven_move_cost_usd_per_1000, 0.0)
        strict = scanner.build_schedule_enhanced_rows(
            schedules=schedules,
            observations=observations,
            rankings=[],
            spike_stats=[],
            end_ts=now,
            baseline_window_days=30,
            baseline_top_k=200,
            require_history_quality_ok=True,
        )
        self.assertEqual(strict, [])

    def test_moves_day_curve_produces_rows(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = [
            scanner.ScheduleEnhancedRow(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="AAA/BBB",
                fee_tier=3000,
                reliability_hit_rate_pct=70.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=2,
                next_add_ts=now + 3600,
                next_remove_ts=now + 10800,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.5,
                baseline_p75_usd_per_1000_hr=0.6,
                gross_block_usd_per_1000=4.0,
                gross_block_usd_per_1000_p90=6.0,
                baseline_block_usd_per_1000=1.0,
                baseline_block_p75_usd_per_1000=1.2,
                incremental_usd_per_1000=3.0,
                incremental_vs_baseline_p75_usd_per_1000=2.8,
                incremental_range_usd_per_1000="3.0..2.8",
                breakeven_move_cost_usd_per_1000=3.0,
                risk_adjusted_incremental_usd_per_1000=1.5,
                tvl_median_usd_est=250000.0,
                max_deployable_usd_est=5000.0,
                deploy_fraction_cap=0.02,
                capacity_flag="OK",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=50.0,
                history_quality="ok",
                nonzero_hours=100,
                confidence_score=0.5,
            )
        ]
        curve = scanner.build_moves_day_curve(
            schedule_rows=rows,
            move_cost_scenarios=[0.0, 25.0],
            deploy_scenarios=[1000.0],
            max_moves_per_day_scenarios=[1, 2],
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            objective="raw",
            schedule_min_max_deployable_usd=1000.0,
            schedule_min_tvl_usd=0.0,
        )
        self.assertEqual(len(curve), 4)
        self.assertTrue(any(row.selected_blocks_count >= 1 for row in curve))

    def test_optimizer_objective_can_change_plan_selection(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = [
            scanner.ScheduleEnhancedRow(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="A/B",
                fee_tier=3000,
                reliability_hit_rate_pct=80.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=1,
                next_add_ts=now + 3600,
                next_remove_ts=now + 7200,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.5,
                baseline_p75_usd_per_1000_hr=0.6,
                gross_block_usd_per_1000=4.0,
                gross_block_usd_per_1000_p90=6.0,
                baseline_block_usd_per_1000=1.0,
                baseline_block_p75_usd_per_1000=1.2,
                incremental_usd_per_1000=3.0,
                incremental_vs_baseline_p75_usd_per_1000=2.8,
                incremental_range_usd_per_1000="3.0..2.8",
                breakeven_move_cost_usd_per_1000=3.0,
                risk_adjusted_incremental_usd_per_1000=0.3,
                tvl_median_usd_est=250000.0,
                max_deployable_usd_est=5000.0,
                deploy_fraction_cap=0.02,
                capacity_flag="OK",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=50.0,
                history_quality="ok",
                nonzero_hours=100,
                confidence_score=0.1,
            ),
            scanner.ScheduleEnhancedRow(
                pool_rank=2,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p2",
                pair="C/D",
                fee_tier=3000,
                reliability_hit_rate_pct=80.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=1,
                next_add_ts=now + 10800,
                next_remove_ts=now + 14400,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.5,
                baseline_p75_usd_per_1000_hr=0.6,
                gross_block_usd_per_1000=2.2,
                gross_block_usd_per_1000_p90=3.0,
                baseline_block_usd_per_1000=1.0,
                baseline_block_p75_usd_per_1000=1.2,
                incremental_usd_per_1000=1.2,
                incremental_vs_baseline_p75_usd_per_1000=1.0,
                incremental_range_usd_per_1000="1.2..1.0",
                breakeven_move_cost_usd_per_1000=1.2,
                risk_adjusted_incremental_usd_per_1000=1.08,
                tvl_median_usd_est=250000.0,
                max_deployable_usd_est=5000.0,
                deploy_fraction_cap=0.02,
                capacity_flag="OK",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=90.0,
                history_quality="ok",
                nonzero_hours=100,
                confidence_score=0.9,
            ),
        ]
        raw_plan = scanner.select_schedule_plan(
            schedule_rows=rows,
            objective="raw",
            move_cost_usd_per_move=0.0,
            deploy_usd=1000.0,
            max_moves_per_day=1,
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            schedule_min_max_deployable_usd=1000.0,
            schedule_min_tvl_usd=0.0,
        )
        risk_plan = scanner.select_schedule_plan(
            schedule_rows=rows,
            objective="risk_adjusted",
            move_cost_usd_per_move=0.0,
            deploy_usd=1000.0,
            max_moves_per_day=1,
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            schedule_min_max_deployable_usd=1000.0,
            schedule_min_tvl_usd=0.0,
        )
        self.assertTrue(raw_plan and risk_plan)
        self.assertNotEqual(raw_plan[0].pool_id, risk_plan[0].pool_id)

    def test_plan_export_integrity_matches_curve_scenario(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = [
            scanner.ScheduleEnhancedRow(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="A/B",
                fee_tier=3000,
                reliability_hit_rate_pct=80.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=1,
                next_add_ts=now + 3600,
                next_remove_ts=now + 7200,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.5,
                baseline_p75_usd_per_1000_hr=0.6,
                gross_block_usd_per_1000=4.0,
                gross_block_usd_per_1000_p90=6.0,
                baseline_block_usd_per_1000=1.0,
                baseline_block_p75_usd_per_1000=1.2,
                incremental_usd_per_1000=3.0,
                incremental_vs_baseline_p75_usd_per_1000=2.8,
                incremental_range_usd_per_1000="3.0..2.8",
                breakeven_move_cost_usd_per_1000=3.0,
                risk_adjusted_incremental_usd_per_1000=1.0,
                tvl_median_usd_est=500000.0,
                max_deployable_usd_est=15000.0,
                deploy_fraction_cap=0.02,
                capacity_flag="OK",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=50.0,
                history_quality="ok",
                nonzero_hours=100,
                confidence_score=0.5,
            )
        ]
        curve = scanner.build_moves_day_curve(
            schedule_rows=rows,
            move_cost_scenarios=[0.0],
            deploy_scenarios=[10000.0],
            max_moves_per_day_scenarios=[4],
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            objective="risk_adjusted",
            schedule_min_max_deployable_usd=1000.0,
            schedule_min_tvl_usd=0.0,
        )
        plan = scanner.select_schedule_plan(
            schedule_rows=rows,
            objective="risk_adjusted",
            move_cost_usd_per_move=0.0,
            deploy_usd=10000.0,
            max_moves_per_day=4,
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            schedule_min_max_deployable_usd=1000.0,
            schedule_min_tvl_usd=0.0,
        )
        self.assertEqual(len(curve), 1)
        self.assertEqual(curve[0].selected_blocks_count, len(plan))

    def test_generate_pool_charts_outputs_png(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            chart_dir = Path(tmpdir) / "charts"
            start = int(dt.datetime(2026, 2, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
            observations = []
            for i in range(48):
                observations.append(
                    scanner.Observation(
                        source_name="s1",
                        version="v3",
                        chain="base",
                        pool_id="0xabc0000000000000000000000000000000000000",
                        pair="AAA/BBB",
                        fee_tier=3000,
                        ts=start + i * 3600,
                        volume_usd=1000.0,
                        tvl_usd=100000.0,
                        fees_usd=10.0 + i,
                        hourly_yield=0.0001 + i * 1e-6,
                    )
                )
            row = scanner.ScheduleEnhancedRow(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="base",
                pool_id="0xabc0000000000000000000000000000000000000",
                pair="AAA/BBB",
                fee_tier=3000,
                reliability_hit_rate_pct=80.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=2,
                next_add_ts=start + 3600,
                next_remove_ts=start + 10800,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.2,
                baseline_p75_usd_per_1000_hr=0.3,
                gross_block_usd_per_1000=4.0,
                gross_block_usd_per_1000_p90=5.0,
                baseline_block_usd_per_1000=1.0,
                baseline_block_p75_usd_per_1000=1.2,
                incremental_usd_per_1000=3.0,
                incremental_vs_baseline_p75_usd_per_1000=2.8,
                incremental_range_usd_per_1000="3.0..2.8",
                breakeven_move_cost_usd_per_1000=3.0,
                risk_adjusted_incremental_usd_per_1000=1.5,
                tvl_median_usd_est=100000.0,
                max_deployable_usd_est=2000.0,
                deploy_fraction_cap=0.02,
                capacity_flag="OK",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=50.0,
                history_quality="ok",
                nonzero_hours=48,
                confidence_score=0.5,
            )
            assets = scanner.generate_pool_charts(
                chart_dir=chart_dir,
                observations=observations,
                schedule_rows=[row],
                end_ts=start + 48 * 3600,
                window_days=30,
                top_n=1,
            )
            self.assertTrue(assets)
            generated = list(chart_dir.glob("*.png"))
            self.assertGreaterEqual(len(generated), 1)

    def test_effective_deploy_capped_by_max_deployable(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = [
            scanner.ScheduleEnhancedRow(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="A/B",
                fee_tier=3000,
                reliability_hit_rate_pct=90.0,
                reliable_occurrences=10,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=1,
                next_add_ts=now + 3600,
                next_remove_ts=now + 7200,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.1,
                baseline_p75_usd_per_1000_hr=0.2,
                gross_block_usd_per_1000=2.0,
                gross_block_usd_per_1000_p90=2.5,
                baseline_block_usd_per_1000=0.5,
                baseline_block_p75_usd_per_1000=0.6,
                incremental_usd_per_1000=1.5,
                incremental_vs_baseline_p75_usd_per_1000=1.4,
                incremental_range_usd_per_1000="1.5..1.4",
                breakeven_move_cost_usd_per_1000=1.5,
                risk_adjusted_incremental_usd_per_1000=1.2,
                tvl_median_usd_est=200000.0,
                max_deployable_usd_est=2500.0,
                deploy_fraction_cap=0.02,
                capacity_flag="LOW_CAPACITY",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=60.0,
                history_quality="ok",
                nonzero_hours=120,
                confidence_score=0.8,
            )
        ]
        plan = scanner.select_schedule_plan(
            schedule_rows=rows,
            objective="risk_adjusted",
            move_cost_usd_per_move=0.0,
            deploy_usd=10000.0,
            max_moves_per_day=1,
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            schedule_min_max_deployable_usd=1000.0,
            schedule_min_tvl_usd=0.0,
        )
        self.assertEqual(len(plan), 1)
        self.assertAlmostEqual(plan[0].effective_deploy_usd, 2500.0, places=6)

    def test_scenario_aware_capacity_allows_small_deploy(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        row = scanner.ScheduleEnhancedRow(
            pool_rank=1,
            source_name="s1",
            version="v3",
            chain="ethereum",
            pool_id="p1",
            pair="A/B",
            fee_tier=3000,
            reliability_hit_rate_pct=80.0,
            reliable_occurrences=10,
            threshold_hourly_yield_pct=0.1,
            threshold_hourly_usd_per_1000_liquidity=1.0,
            avg_block_hourly_yield_pct=0.2,
            p90_block_hourly_yield_pct=0.3,
            avg_block_hourly_usd_per_1000_liquidity=2.0,
            p90_block_hourly_usd_per_1000_liquidity=3.0,
            block_hours=1,
            next_add_ts=now + 3600,
            next_remove_ts=now + 7200,
            pool_score=1.0,
            baseline_usd_per_1000_hr=0.1,
            baseline_p75_usd_per_1000_hr=0.2,
            gross_block_usd_per_1000=2.0,
            gross_block_usd_per_1000_p90=2.5,
            baseline_block_usd_per_1000=0.5,
            baseline_block_p75_usd_per_1000=0.6,
            incremental_usd_per_1000=1.5,
            incremental_vs_baseline_p75_usd_per_1000=1.4,
            incremental_range_usd_per_1000="1.5..1.4",
            breakeven_move_cost_usd_per_1000=1.5,
            risk_adjusted_incremental_usd_per_1000=1.2,
            tvl_median_usd_est=100000.0,
            max_deployable_usd_est=2500.0,
            deploy_fraction_cap=0.02,
            capacity_flag="LOW_CAPACITY",
            run_length_p50=2.0,
            run_length_p90=3.0,
            hit_rate_pct=60.0,
            history_quality="ok",
            nonzero_hours=120,
            confidence_score=0.8,
        )
        plan = scanner.select_schedule_plan(
            schedule_rows=[row],
            objective="risk_adjusted",
            move_cost_usd_per_move=0.0,
            deploy_usd=1000.0,
            max_moves_per_day=1,
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            schedule_min_max_deployable_usd=10000.0,
            schedule_absolute_min_max_deployable_usd=0.0,
            schedule_min_tvl_usd=0.0,
        )
        self.assertEqual(len(plan), 1)

    def test_schedule_run_diagnostics_reports_capacity_zero_stage(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        row = scanner.ScheduleEnhancedRow(
            pool_rank=1,
            source_name="s1",
            version="v3",
            chain="ethereum",
            pool_id="p1",
            pair="A/B",
            fee_tier=3000,
            reliability_hit_rate_pct=80.0,
            reliable_occurrences=10,
            threshold_hourly_yield_pct=0.1,
            threshold_hourly_usd_per_1000_liquidity=1.0,
            avg_block_hourly_yield_pct=0.2,
            p90_block_hourly_yield_pct=0.3,
            avg_block_hourly_usd_per_1000_liquidity=2.0,
            p90_block_hourly_usd_per_1000_liquidity=3.0,
            block_hours=1,
            next_add_ts=now + 3600,
            next_remove_ts=now + 7200,
            pool_score=1.0,
            baseline_usd_per_1000_hr=0.1,
            baseline_p75_usd_per_1000_hr=0.2,
            gross_block_usd_per_1000=2.0,
            gross_block_usd_per_1000_p90=2.5,
            baseline_block_usd_per_1000=0.5,
            baseline_block_p75_usd_per_1000=0.6,
            incremental_usd_per_1000=1.5,
            incremental_vs_baseline_p75_usd_per_1000=1.4,
            incremental_range_usd_per_1000="1.5..1.4",
            breakeven_move_cost_usd_per_1000=1.5,
            risk_adjusted_incremental_usd_per_1000=1.2,
            tvl_median_usd_est=100000.0,
            max_deployable_usd_est=500.0,
            deploy_fraction_cap=0.02,
            capacity_flag="LOW_CAPACITY",
            run_length_p50=2.0,
            run_length_p90=3.0,
            hit_rate_pct=60.0,
            history_quality="ok",
            nonzero_hours=120,
            confidence_score=0.8,
        )
        diags = scanner.build_schedule_run_diagnostics(
            schedule_rows=[row],
            objective="risk_adjusted",
            move_cost_scenarios=[0.0],
            deploy_scenarios=[1000.0],
            max_moves_per_day_scenarios=[1],
            min_hold_hours=1,
            cooldown_hours_between_moves=0,
            schedule_min_max_deployable_usd=10000.0,
            schedule_absolute_min_max_deployable_usd=0.0,
            schedule_min_tvl_usd=0.0,
            excluded_by_source_health=0,
        )
        self.assertEqual(len(diags), 1)
        self.assertEqual(diags[0].selected_blocks_count, 0)
        self.assertEqual(diags[0].reason_if_zero, "excluded_by_capacity")

    def test_dashboard_state_json_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "dashboard_state.json"
            state_obj = scanner.write_dashboard_state_json(
                path=out,
                generated_ts=int(dt.datetime(2026, 2, 25, 0, 0, tzinfo=dt.timezone.utc).timestamp()),
                llama_diagnostics=None,
                llama_thresholds=None,
                llama_rows=[],
                schedule_rows=[],
                selected_plan_rows=[],
                source_health_rows=[],
                moves_day_curve=[],
                schedule_run_diagnostics=[],
                scenario_plan_filename="selected_plan_risk_adjusted_cost50_deploy10000_moves4.csv",
                default_move_cost_usd=50.0,
                default_deploy_usd=10000.0,
                default_max_moves_per_day=4,
                optimizer_objective="risk_adjusted",
                chart_assets={},
            )
            self.assertTrue(out.exists())
            state = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(state["defaults"]["objective"], "risk_adjusted")
            self.assertEqual(state["defaults"]["max_moves_per_day"], 4)
            html_out = Path(tmpdir) / "dashboard.html"
            scanner.write_dashboard_html(html_out, state_obj)
            self.assertTrue(html_out.exists())
            html_text = html_out.read_text(encoding="utf-8")
            self.assertIn("Yield Scanner Dashboard", html_text)
            self.assertIn("Live Spikes Heatmap", html_text)

    def test_confidence_distribution_non_constant(self) -> None:
        rows = [
            scanner.ScheduleEnhancedRow(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="A/B",
                fee_tier=3000,
                reliability_hit_rate_pct=90.0,
                reliable_occurrences=20,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=1,
                next_add_ts=0,
                next_remove_ts=3600,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.1,
                baseline_p75_usd_per_1000_hr=0.2,
                gross_block_usd_per_1000=2.0,
                gross_block_usd_per_1000_p90=2.5,
                baseline_block_usd_per_1000=0.5,
                baseline_block_p75_usd_per_1000=0.6,
                incremental_usd_per_1000=1.5,
                incremental_vs_baseline_p75_usd_per_1000=1.4,
                incremental_range_usd_per_1000="1.5..1.4",
                breakeven_move_cost_usd_per_1000=1.5,
                risk_adjusted_incremental_usd_per_1000=1.2,
                tvl_median_usd_est=100000.0,
                max_deployable_usd_est=2500.0,
                deploy_fraction_cap=0.02,
                capacity_flag="LOW_CAPACITY",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=60.0,
                history_quality="ok",
                nonzero_hours=168,
                confidence_score=0.8,
            ),
            scanner.ScheduleEnhancedRow(
                pool_rank=2,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p2",
                pair="C/D",
                fee_tier=3000,
                reliability_hit_rate_pct=40.0,
                reliable_occurrences=2,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=1,
                next_add_ts=7200,
                next_remove_ts=10800,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.1,
                baseline_p75_usd_per_1000_hr=0.2,
                gross_block_usd_per_1000=2.0,
                gross_block_usd_per_1000_p90=2.5,
                baseline_block_usd_per_1000=0.5,
                baseline_block_p75_usd_per_1000=0.6,
                incremental_usd_per_1000=1.5,
                incremental_vs_baseline_p75_usd_per_1000=1.4,
                incremental_range_usd_per_1000="1.5..1.4",
                breakeven_move_cost_usd_per_1000=1.5,
                risk_adjusted_incremental_usd_per_1000=0.1,
                tvl_median_usd_est=100000.0,
                max_deployable_usd_est=2500.0,
                deploy_fraction_cap=0.02,
                capacity_flag="LOW_CAPACITY",
                run_length_p50=1.0,
                run_length_p90=1.0,
                hit_rate_pct=5.0,
                history_quality="ok",
                nonzero_hours=12,
                confidence_score=0.02,
            ),
        ]
        stddev = statistics.pstdev([r.confidence_score for r in rows])
        self.assertGreater(stddev, 1e-6)

    def test_rank_pools_orders_by_earning_potential_and_finds_best_window(self) -> None:
        start = int(dt.datetime(2025, 1, 6, 0, 0, tzinfo=dt.timezone.utc).timestamp())

        rows = []
        for day in range(7):
            for hour in range(24):
                ts = start + (day * 24 + hour) * 3600

                y1 = 0.0005
                rows.append(
                    Observation(
                        source_name="s1",
                        version="v3",
                        chain="ethereum",
                        pool_id="pool-1",
                        pair="AAA/BBB",
                        fee_tier=500,
                        ts=ts,
                        volume_usd=100000.0,
                        tvl_usd=1_000_000.0,
                        fees_usd=y1 * 1_000_000.0,
                        hourly_yield=y1,
                    )
                )

                y2 = 0.0008
                if day == 2 and hour == 14:
                    y2 = 0.0032
                rows.append(
                    Observation(
                        source_name="s1",
                        version="v3",
                        chain="ethereum",
                        pool_id="pool-2",
                        pair="CCC/DDD",
                        fee_tier=3000,
                        ts=ts,
                        volume_usd=160000.0,
                        tvl_usd=1_000_000.0,
                        fees_usd=y2 * 1_000_000.0,
                        hourly_yield=y2,
                    )
                )

        rankings = rank_pools(rows, min_samples=24)
        self.assertEqual(len(rankings), 2)
        self.assertEqual(rankings[0].pool_id, "pool-2")
        self.assertEqual(rankings[0].best_day_utc, "Wednesday")
        self.assertEqual(rankings[0].best_hour_utc, 14)
        self.assertEqual(rankings[0].observed_hours, 168)
        self.assertAlmostEqual(rankings[0].observed_days, 7.0)
        expected_start = start + (2 * 24 + 14) * 3600
        self.assertEqual(rankings[0].best_window_start_ts, expected_start)
        self.assertEqual(rankings[0].best_window_end_ts, expected_start + 3600)

    def test_rank_pools_writes_diagnostics_and_applies_winsorization(self) -> None:
        start = int(dt.datetime(2025, 1, 6, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = []
        for hour in range(48):
            ts = start + hour * 3600
            y = 0.001
            if hour == 47:
                y = 0.2
            rows.append(
                Observation(
                    source_name="s1",
                    version="v3",
                    chain="ethereum",
                    pool_id="pool-x",
                    pair="AAA/BBB",
                    fee_tier=3000,
                    ts=ts,
                    volume_usd=100000.0,
                    tvl_usd=100000.0,
                    fees_usd=y * 100000.0,
                    hourly_yield=y,
                )
            )
        diags: list[scanner.PoolRankingDiagnostic] = []
        rankings = rank_pools(
            rows,
            min_samples=24,
            ranking_stats_min_tvl_usd=50000.0,
            winsorize_percentile=0.95,
            diagnostics_out=diags,
        )
        self.assertEqual(len(rankings), 1)
        self.assertEqual(len(diags), 1)
        self.assertGreater(diags[0].capped_hours, 0)
        self.assertLess(diags[0].max_capped_hourly_yield_pct, diags[0].max_raw_hourly_yield_pct)

    def test_rank_pools_hard_cap_rule_only_when_rows_removed(self) -> None:
        start = int(dt.datetime(2025, 1, 6, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = []
        for hour in range(48):
            ts = start + hour * 3600
            y = 0.001
            rows.append(
                Observation(
                    source_name="s1",
                    version="v3",
                    chain="ethereum",
                    pool_id="pool-x",
                    pair="AAA/BBB",
                    fee_tier=3000,
                    ts=ts,
                    volume_usd=100000.0,
                    tvl_usd=100000.0,
                    fees_usd=y * 100000.0,
                    hourly_yield=y,
                )
            )
        diags: list[scanner.PoolRankingDiagnostic] = []
        rankings = rank_pools(
            rows,
            min_samples=24,
            max_hourly_yield_pct=100.0,
            diagnostics_out=diags,
        )
        self.assertEqual(len(rankings), 1)
        self.assertEqual(len(diags), 1)
        self.assertNotIn("hard_cap", diags[0].rule_triggered)

    def test_normalize_row_derives_fees_when_missing(self) -> None:
        source = SourceConfig(
            name="v4-test",
            version="v4",
            chain="ethereum",
            endpoint="https://example.invalid/graphql",
            hourly_query="query {}",
        )
        row = {
            "ts": 1700000000,
            "volumeUSD": "100000",
            "tvlUSD": "2000000",
            "pool": {
                "id": "0xpool",
                "feeTier": "3000",
                "currency0": {"symbol": "ETH"},
                "currency1": {"symbol": "USDC"},
            },
        }

        obs = normalize_row(source, row)
        self.assertIsNotNone(obs)
        assert obs is not None
        self.assertEqual(obs.pool_id, "0xpool")
        self.assertEqual(obs.pair, "ETH/USDC")
        self.assertAlmostEqual(obs.fees_usd, 300.0)
        self.assertAlmostEqual(obs.hourly_yield or 0.0, 0.00015)

    def test_normalize_row_does_not_derive_dynamic_fee_tier(self) -> None:
        source = SourceConfig(
            name="v4-test",
            version="v4",
            chain="ethereum",
            endpoint="https://example.invalid/graphql",
            hourly_query="query {}",
        )
        row = {
            "ts": 1700000000,
            "volumeUSD": "100000",
            "tvlUSD": "2000000",
            "pool": {
                "id": "0xpool",
                "feeTier": "8388608",
                "token0": {"symbol": "WETH"},
                "token1": {"symbol": "USDC"},
            },
        }

        obs = normalize_row(source, row)
        self.assertIsNotNone(obs)
        assert obs is not None
        self.assertIsNone(obs.fees_usd)
        self.assertIsNone(obs.hourly_yield)

    def test_normalize_row_does_not_derive_high_near_one_fee_tier(self) -> None:
        source = SourceConfig(
            name="v4-test",
            version="v4",
            chain="ethereum",
            endpoint="https://example.invalid/graphql",
            hourly_query="query {}",
        )
        row = {
            "ts": 1700000000,
            "volumeUSD": "100000",
            "tvlUSD": "2000000",
            "pool": {
                "id": "0xpool",
                "feeTier": "999610",
                "token0": {"symbol": "WETH"},
                "token1": {"symbol": "USDC"},
            },
        }

        obs = normalize_row(source, row)
        self.assertIsNotNone(obs)
        assert obs is not None
        self.assertIsNone(obs.fees_usd)
        self.assertIsNone(obs.hourly_yield)

    def test_normalize_row_uses_symbol_alias_when_symbol_missing(self) -> None:
        source = SourceConfig(
            name="v2-test",
            version="v2",
            chain="ethereum",
            endpoint="https://example.invalid/graphql",
            hourly_query="query {}",
        )
        row = {
            "ts": 1700000000,
            "volumeUSD": "1000",
            "tvlUSD": "100000",
            "pool": {
                "id": "0xpool",
                "feeTier": "3000",
                "token0": {"id": "0x6b175474e89094c44da98b954eedeac495271d0f"},
                "token1": {"id": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"},
            },
        }
        obs = normalize_row(source, row)
        self.assertIsNotNone(obs)
        assert obs is not None
        self.assertEqual(obs.pair, "DAI/WETH")

    def test_quality_filter_rejects_high_implied_fee_rate_for_non_v2_spike(self) -> None:
        rows = [
            Observation(
                source_name="uniswap-v4-mainnet-official",
                version="v4",
                chain="ethereum",
                pool_id="pool-1",
                pair="ETH/USDC",
                fee_tier=999610,
                ts=1700000000,
                volume_usd=1000.0,
                tvl_usd=50000.0,
                fees_usd=500.0,
                hourly_yield=0.01,
            ),
            Observation(
                source_name="sushi-v2-fee-spikes-mainnet",
                version="v2",
                chain="ethereum",
                pool_id="pool-2",
                pair="DAI/WETH",
                fee_tier=3000,
                ts=1700000000,
                volume_usd=1000.0,
                tvl_usd=50000.0,
                fees_usd=500.0,
                hourly_yield=0.01,
            ),
        ]
        filtered, rejected = scanner.filter_observations_with_quality_audit(
            observations=rows,
            min_tvl_usd=0.0,
            max_hourly_yield_pct=None,
            v2_spike_sources={"sushi-v2-fee-spikes-mainnet"},
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].source_name, "sushi-v2-fee-spikes-mainnet")
        self.assertEqual(
            rejected.get(("uniswap-v4-mainnet-official", "v4", "ethereum", "invalid_fee_tier")),
            1,
        )

    def test_quality_filter_respects_custom_implied_fee_rate_cap(self) -> None:
        rows = [
            Observation(
                source_name="uniswap-v3-mainnet-official",
                version="v3",
                chain="ethereum",
                pool_id="pool-1",
                pair="ETH/USDC",
                fee_tier=3000,
                ts=1700000000,
                volume_usd=1000.0,
                tvl_usd=50000.0,
                fees_usd=60.0,  # 6% implied fee rate
                hourly_yield=0.0012,
            )
        ]
        filtered, rejected = scanner.filter_observations_with_quality_audit(
            observations=rows,
            min_tvl_usd=0.0,
            max_hourly_yield_pct=None,
            v2_spike_sources=set(),
            max_implied_fee_rate=0.05,
        )
        self.assertEqual(len(filtered), 0)
        self.assertEqual(
            rejected.get(("uniswap-v3-mainnet-official", "v3", "ethereum", "implied_fee_rate_gt_cap")),
            1,
        )

    def test_schedule_relaxation_fallback_increases_coverage(self) -> None:
        start = int(dt.datetime(2026, 1, 1, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[Observation] = []
        for day in range(28):
            for hour in [10]:
                ts = start + (day * 24 + hour) * 3600
                # About half of weekly-hour occurrences exceed fixed threshold.
                y = 0.002 if (day % 4 in {0, 1}) else 0.0002
                rows.append(
                    Observation(
                        source_name="s1",
                        version="v3",
                        chain="ethereum",
                        pool_id="pool-schedule",
                        pair="AAA/BBB",
                        fee_tier=3000,
                        ts=ts,
                        volume_usd=10000.0,
                        tvl_usd=1_000_000.0,
                        fees_usd=y * 1_000_000.0,
                        hourly_yield=y,
                    )
                )
        rankings = rank_pools(rows, min_samples=4)
        schedules, trace = scanner.build_liquidity_schedule_with_relaxation(
            rankings=rankings,
            observations=rows,
            end_ts=start + 28 * 24 * 3600,
            top_pools=1,
            quantile=0.75,
            min_usd_per_1000_hour=1.0,
            base_min_hit_rate=0.60,
            base_min_occurrences=2,
            max_blocks_per_pool=3,
            min_observed_days=0.0,
            min_block_target=1,
            enable_relaxation=True,
        )
        self.assertTrue(schedules)
        self.assertIn("relax_hit_0.10", trace)

    def test_persistent_source_quarantine_tracks_bad_run_streak(self) -> None:
        row = scanner.SourceHealthRow(
            source_name="bad-source",
            version="v4",
            chain="base",
            input_rows=100,
            fees_with_nonpositive_tvl_input_count=50,
            fees_with_nonpositive_tvl_rate=0.50,
            tvl_below_floor_count=10,
            tvl_below_floor_rate=0.10,
            invalid_fee_tier_count=5,
            invalid_fee_tier_rate=0.05,
            implied_fee_anomaly_count=8,
            implied_fee_anomaly_rate=0.08,
            bad_run_streak=0,
            persistent_anomaly_excluded=False,
            excluded_from_schedule=True,
            exclusion_reason="fees_with_nonpositive_tvl_rate 0.5000 > 0.1000",
        )
        rows1, hist1 = scanner.apply_persistent_source_quarantine(
            rows=[row],
            history={},
            persistent_runs=2,
            now_ts=1_700_000_000,
        )
        self.assertEqual(rows1[0].bad_run_streak, 1)
        self.assertFalse(rows1[0].persistent_anomaly_excluded)
        rows2, _ = scanner.apply_persistent_source_quarantine(
            rows=[row],
            history=hist1,
            persistent_runs=2,
            now_ts=1_700_000_000 + 3600,
        )
        self.assertEqual(rows2[0].bad_run_streak, 2)
        self.assertTrue(rows2[0].persistent_anomaly_excluded)
        self.assertIn("persistent_anomaly_streak", rows2[0].exclusion_reason)

    def test_parse_source_key_overrides_accepts_common_delimiters(self) -> None:
        parsed = scanner.parse_source_key_overrides(
            [
                "s1|v3|ethereum",
                "s2:v4:base",
                "s3,v2,bnb",
                "invalid-format",
            ]
        )
        self.assertIn(("s1", "v3", "ethereum"), parsed)
        self.assertIn(("s2", "v4", "base"), parsed)
        self.assertIn(("s3", "v2", "bnb"), parsed)
        self.assertNotIn(("invalid-format", "", ""), parsed)

    def test_apply_source_health_overrides_include_and_exclude(self) -> None:
        rows = [
            scanner.SourceHealthRow(
                source_name="s1",
                version="v3",
                chain="ethereum",
                input_rows=10,
                fees_with_nonpositive_tvl_input_count=3,
                fees_with_nonpositive_tvl_rate=0.3,
                tvl_below_floor_count=2,
                tvl_below_floor_rate=0.2,
                invalid_fee_tier_count=0,
                invalid_fee_tier_rate=0.0,
                implied_fee_anomaly_count=0,
                implied_fee_anomaly_rate=0.0,
                bad_run_streak=2,
                persistent_anomaly_excluded=False,
                excluded_from_schedule=True,
                exclusion_reason="bad_rate",
            ),
            scanner.SourceHealthRow(
                source_name="s2",
                version="v4",
                chain="base",
                input_rows=10,
                fees_with_nonpositive_tvl_input_count=0,
                fees_with_nonpositive_tvl_rate=0.0,
                tvl_below_floor_count=0,
                tvl_below_floor_rate=0.0,
                invalid_fee_tier_count=0,
                invalid_fee_tier_rate=0.0,
                implied_fee_anomaly_count=0,
                implied_fee_anomaly_rate=0.0,
                bad_run_streak=0,
                persistent_anomaly_excluded=False,
                excluded_from_schedule=False,
                exclusion_reason="",
            ),
        ]
        updated = scanner.apply_source_health_overrides(
            rows=rows,
            force_include={("s1", "v3", "ethereum")},
            force_exclude={("s2", "v4", "base")},
        )
        by_key = {(r.source_name, r.version, r.chain): r for r in updated}
        self.assertFalse(by_key[("s1", "v3", "ethereum")].excluded_from_schedule)
        self.assertIn("manual_include_override", by_key[("s1", "v3", "ethereum")].exclusion_reason)
        self.assertTrue(by_key[("s2", "v4", "base")].excluded_from_schedule)
        self.assertIn("manual_exclude_override", by_key[("s2", "v4", "base")].exclusion_reason)

    def test_resolve_default_deploy_usd_auto_mode_uses_capacity_percentile(self) -> None:
        now = int(dt.datetime(2026, 2, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = [
            scanner.ScheduleEnhancedRow(
                pool_rank=1,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p1",
                pair="AAA/BBB",
                fee_tier=3000,
                reliability_hit_rate_pct=70.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=2,
                next_add_ts=now + 3600,
                next_remove_ts=now + 10800,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.5,
                baseline_p75_usd_per_1000_hr=0.6,
                gross_block_usd_per_1000=4.0,
                gross_block_usd_per_1000_p90=6.0,
                baseline_block_usd_per_1000=1.0,
                baseline_block_p75_usd_per_1000=1.2,
                incremental_usd_per_1000=3.0,
                incremental_vs_baseline_p75_usd_per_1000=2.8,
                incremental_range_usd_per_1000="3.0..2.8",
                breakeven_move_cost_usd_per_1000=3.0,
                risk_adjusted_incremental_usd_per_1000=1.5,
                tvl_median_usd_est=250000.0,
                max_deployable_usd_est=2000.0,
                deploy_fraction_cap=0.02,
                capacity_flag="OK",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=50.0,
                history_quality="ok",
                nonzero_hours=100,
                confidence_score=0.5,
            ),
            scanner.ScheduleEnhancedRow(
                pool_rank=2,
                source_name="s1",
                version="v3",
                chain="ethereum",
                pool_id="p2",
                pair="CCC/DDD",
                fee_tier=3000,
                reliability_hit_rate_pct=70.0,
                reliable_occurrences=3,
                threshold_hourly_yield_pct=0.1,
                threshold_hourly_usd_per_1000_liquidity=1.0,
                avg_block_hourly_yield_pct=0.2,
                p90_block_hourly_yield_pct=0.3,
                avg_block_hourly_usd_per_1000_liquidity=2.0,
                p90_block_hourly_usd_per_1000_liquidity=3.0,
                block_hours=2,
                next_add_ts=now + 3600,
                next_remove_ts=now + 10800,
                pool_score=1.0,
                baseline_usd_per_1000_hr=0.5,
                baseline_p75_usd_per_1000_hr=0.6,
                gross_block_usd_per_1000=4.0,
                gross_block_usd_per_1000_p90=6.0,
                baseline_block_usd_per_1000=1.0,
                baseline_block_p75_usd_per_1000=1.2,
                incremental_usd_per_1000=3.0,
                incremental_vs_baseline_p75_usd_per_1000=2.8,
                incremental_range_usd_per_1000="3.0..2.8",
                breakeven_move_cost_usd_per_1000=3.0,
                risk_adjusted_incremental_usd_per_1000=1.5,
                tvl_median_usd_est=250000.0,
                max_deployable_usd_est=6000.0,
                deploy_fraction_cap=0.02,
                capacity_flag="OK",
                run_length_p50=2.0,
                run_length_p90=3.0,
                hit_rate_pct=50.0,
                history_quality="ok",
                nonzero_hours=100,
                confidence_score=0.5,
            ),
        ]
        resolved = scanner.resolve_default_deploy_usd(
            rows=rows,
            mode="auto_p50",
            fixed_value=10000.0,
            auto_min_usd=1000.0,
            auto_max_usd=50000.0,
        )
        self.assertGreaterEqual(resolved, 2000.0)
        self.assertLessEqual(resolved, 6000.0)

    def test_choose_fallback_curve_row_prefers_nonzero_move_cost(self) -> None:
        rows = [
            scanner.MovesDayCurveRow(
                objective="risk_adjusted",
                move_cost_usd_per_move=0.0,
                deploy_usd=10000.0,
                max_moves_per_day=4,
                min_hold_hours=1,
                cooldown_hours_between_moves=0,
                selected_blocks_count=10,
                selected_moves_count=10,
                total_net_usd=200.0,
                total_gross_usd=220.0,
                total_baseline_usd=20.0,
                notes="ok",
            ),
            scanner.MovesDayCurveRow(
                objective="risk_adjusted",
                move_cost_usd_per_move=50.0,
                deploy_usd=10000.0,
                max_moves_per_day=4,
                min_hold_hours=1,
                cooldown_hours_between_moves=0,
                selected_blocks_count=8,
                selected_moves_count=8,
                total_net_usd=150.0,
                total_gross_usd=200.0,
                total_baseline_usd=50.0,
                notes="ok",
            ),
        ]
        picked = scanner.choose_fallback_curve_row(
            rows=rows,
            objective="risk_adjusted",
            default_move_cost_usd=50.0,
            default_deploy_usd=10000.0,
            default_max_moves_per_day=4,
        )
        self.assertIsNotNone(picked)
        assert picked is not None
        self.assertEqual(picked.move_cost_usd_per_move, 50.0)

    def test_llama_pair_hour_query_uses_hourly_alias(self) -> None:
        self.assertIn("hourly: pairHourDatas", scanner.V2_PAIR_HOUR_QUERY)

    def test_v2_spike_fetch_accepts_hourly_alias(self) -> None:
        source = SourceConfig(
            name="sushi-v2-fee-spikes-mainnet",
            version="v2",
            chain="ethereum",
            endpoint="https://example.invalid",
            hourly_query="",
            source_type="v2_spike",
        )
        row = {
            "id": "hour-row-1",
            "hourStartUnix": 1_700_000_000,
            "fee0": "1.0",
            "fee1": "0",
            "reserve0": "100.0",
            "reserve1": "200.0",
            "swapCount": "20",
            "pair": {"id": "0xpair"},
        }
        with patch("scanner.graphql_query", return_value={"hourly": [row]}), patch(
            "scanner._fetch_v2_pair_metadata",
            return_value={"0xpair": (source.weth_address.lower(), "0xtoken")},
        ):
            obs = scanner.fetch_v2_spike_observations(
                source=source,
                start_ts=1_699_999_000,
                end_ts=1_700_001_000,
                page_size=1000,
                max_pages=1,
                timeout=30,
                retries=1,
                min_swap_count=1,
                min_reserve_weth=10.0,
            )
        self.assertEqual(len(obs), 1)
        self.assertEqual(obs[0].pool_id, "0xpair")
        self.assertEqual(obs[0].source_name, "sushi-v2-fee-spikes-mainnet")

    def test_build_liquidity_schedule_finds_reliable_recurring_block(self) -> None:
        start = int(dt.datetime(2025, 1, 6, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows = []
        for day in range(21):  # three weeks
            for hour in range(24):
                ts = start + (day * 24 + hour) * 3600
                y = 0.00005
                # Reliable recurring high-yield window: Tuesday 10:00-12:00 UTC
                if (day % 7) == 1 and hour in {10, 11}:
                    y = 0.001
                rows.append(
                    Observation(
                        source_name="s1",
                        version="v3",
                        chain="ethereum",
                        pool_id="pool-schedule",
                        pair="AAA/BBB",
                        fee_tier=3000,
                        ts=ts,
                        volume_usd=100000.0,
                        tvl_usd=1_000_000.0,
                        fees_usd=y * 1_000_000.0,
                        hourly_yield=y,
                    )
                )

        rankings = rank_pools(rows, min_samples=24)
        end_ts = start + (21 * 24 * 3600)
        schedule = scanner.build_liquidity_schedule(
            rankings=rankings,
            observations=rows,
            end_ts=end_ts,
            top_pools=1,
            quantile=0.999,
            min_usd_per_1000_hour=None,
            min_hit_rate=0.90,
            min_occurrences=3,
            max_blocks_per_pool=3,
        )
        self.assertTrue(schedule)
        block = schedule[0]
        self.assertEqual(block.add_day_utc, "Tuesday")
        self.assertEqual(block.add_hour_utc, 10)
        self.assertEqual(block.remove_day_utc, "Tuesday")
        self.assertEqual(block.remove_hour_utc, 12)
        self.assertEqual(block.block_hours, 2)

        next_add = dt.datetime.fromtimestamp(block.next_add_ts, tz=dt.timezone.utc)
        next_remove = dt.datetime.fromtimestamp(block.next_remove_ts, tz=dt.timezone.utc)
        self.assertEqual(next_add.weekday(), 1)  # Tuesday
        self.assertEqual(next_add.hour, 10)
        self.assertEqual(next_remove - next_add, dt.timedelta(hours=2))

    def test_resolve_endpoint_rejects_placeholder(self) -> None:
        with self.assertRaises(ValueError):
            resolve_endpoint("https://<replace-me>", "source[1]")

    def test_resolve_endpoint_requires_env_variable(self) -> None:
        with self.assertRaises(ValueError):
            resolve_endpoint("${MISSING_ENDPOINT_VAR}", "source[1]")

    def test_fetch_all_observations_continues_when_not_strict(self) -> None:
        sources = [
            SourceConfig(
                name="bad",
                version="v4",
                chain="ethereum",
                endpoint="https://example.invalid",
                hourly_query="query {}",
            ),
            SourceConfig(
                name="good",
                version="v3",
                chain="ethereum",
                endpoint="https://example.invalid",
                hourly_query="query {}",
            ),
        ]

        def fake_fetch(
            source: SourceConfig,
            start_ts: int,
            end_ts: int,
            page_size: int,
            max_pages: int | None,
            timeout: int,
            retries: int,
            v2_spike_min_swap_count: int,
            v2_spike_min_reserve_weth: float,
        ) -> list[Observation]:
            if source.name == "bad":
                raise RuntimeError("boom")
            return [
                Observation(
                    source_name=source.name,
                    version=source.version,
                    chain=source.chain,
                    pool_id="pool",
                    pair="A/B",
                    fee_tier=500,
                    ts=1700000000,
                    volume_usd=100.0,
                    tvl_usd=1000.0,
                    fees_usd=1.0,
                    hourly_yield=0.001,
                )
            ]

        original = scanner.fetch_source_observations
        scanner.fetch_source_observations = fake_fetch
        try:
            obs, failures = scanner.fetch_all_observations(
                sources=sources,
                start_ts=1,
                end_ts=2,
                page_size=100,
                max_pages=None,
                workers=2,
                parallel_window_hours=24,
                timeout=1,
                retries=1,
                strict_sources=False,
                v2_spike_min_swap_count=10,
                v2_spike_min_reserve_weth=10.0,
            )
        finally:
            scanner.fetch_source_observations = original

        self.assertEqual(len(obs), 1)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].source_name, "bad")

    def test_build_v2_spike_rows_filters_and_computes_metrics(self) -> None:
        ts = int(dt.datetime(2026, 2, 22, 4, 0, tzinfo=dt.timezone.utc).timestamp())
        obs = [
            Observation(
                source_name="sushi-v2-fee-spikes-mainnet",
                version="v2",
                chain="ethereum",
                pool_id="0xpair",
                pair="0xc02aa39b223fe8d0a0e5c4f27ead9083c756cc2/0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                fee_tier=3000,
                ts=ts,
                volume_usd=12.0,
                tvl_usd=100.0,
                fees_usd=0.5,
                hourly_yield=0.005,
            ),
            # below swap threshold
            Observation(
                source_name="sushi-v2-fee-spikes-mainnet",
                version="v2",
                chain="ethereum",
                pool_id="0xpair2",
                pair="0xc02aa39b223fe8d0a0e5c4f27ead9083c756cc2/0xdac17f958d2ee523a2206206994597c13d831ec7",
                fee_tier=3000,
                ts=ts,
                volume_usd=5.0,
                tvl_usd=100.0,
                fees_usd=0.4,
                hourly_yield=0.004,
            ),
        ]

        rows = scanner.build_v2_spike_rows(
            observations=obs,
            v2_spike_sources={"sushi-v2-fee-spikes-mainnet"},
            min_swap_count=10,
            min_reserve_weth=10.0,
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.pair_id, "0xpair")
        self.assertEqual(row.swap_count, 12)
        self.assertAlmostEqual(row.score, 0.005)
        self.assertAlmostEqual(row.hourly_yield_pct, 0.5)
        self.assertAlmostEqual(row.rough_apr_pct, 4380.0)

    def test_to_bool_string_false_is_false(self) -> None:
        self.assertFalse(scanner.to_bool("false"))
        self.assertTrue(scanner.to_bool("true"))

    def test_resolve_env_string_requires_env_variable(self) -> None:
        with self.assertRaises(ValueError):
            scanner.resolve_env_string("Bearer ${MISSING_TOKEN}", "header auth")

    def test_usd_per_1000_from_yield_pct(self) -> None:
        self.assertAlmostEqual(scanner.usd_per_1000_from_yield_pct(0.15), 1.5)
        self.assertAlmostEqual(scanner.usd_per_1000_from_yield_pct(2.0), 20.0)

    def test_select_jump_now_schedules_prefers_active_and_soon(self) -> None:
        now = int(dt.datetime(2026, 2, 23, 10, 0, tzinfo=dt.timezone.utc).timestamp())  # Monday

        active = scanner.ScheduleRecommendation(
            pool_rank=1,
            source_name="s1",
            version="v4",
            chain="ethereum",
            pool_id="p1",
            pair="ETH/USDC",
            fee_tier=3000,
            reliability_hit_rate_pct=80.0,
            reliable_occurrences=3,
            threshold_hourly_yield_pct=0.1,
            avg_block_hourly_yield_pct=0.2,
            p90_block_hourly_yield_pct=0.25,
            block_hours=2,
            add_day_utc="Monday",
            add_hour_utc=10,
            remove_day_utc="Monday",
            remove_hour_utc=12,
            add_pattern_utc="Monday 10:00 UTC",
            remove_pattern_utc="Monday 12:00 UTC",
            next_add_ts=now + (7 * 24 * 3600),
            next_remove_ts=now + (7 * 24 * 3600) + (2 * 3600),
            pool_score=1.0,
        )
        soon = scanner.ScheduleRecommendation(
            pool_rank=2,
            source_name="s1",
            version="v4",
            chain="base",
            pool_id="p2",
            pair="WETH/USDC",
            fee_tier=3000,
            reliability_hit_rate_pct=70.0,
            reliable_occurrences=2,
            threshold_hourly_yield_pct=0.1,
            avg_block_hourly_yield_pct=0.15,
            p90_block_hourly_yield_pct=0.2,
            block_hours=1,
            add_day_utc="Monday",
            add_hour_utc=11,
            remove_day_utc="Monday",
            remove_hour_utc=12,
            add_pattern_utc="Monday 11:00 UTC",
            remove_pattern_utc="Monday 12:00 UTC",
            next_add_ts=now + 3600,
            next_remove_ts=now + 7200,
            pool_score=0.9,
        )
        later = scanner.ScheduleRecommendation(
            pool_rank=3,
            source_name="s1",
            version="v3",
            chain="arbitrum",
            pool_id="p3",
            pair="ETH/USDT",
            fee_tier=500,
            reliability_hit_rate_pct=60.0,
            reliable_occurrences=2,
            threshold_hourly_yield_pct=0.1,
            avg_block_hourly_yield_pct=0.12,
            p90_block_hourly_yield_pct=0.13,
            block_hours=1,
            add_day_utc="Tuesday",
            add_hour_utc=10,
            remove_day_utc="Tuesday",
            remove_hour_utc=11,
            add_pattern_utc="Tuesday 10:00 UTC",
            remove_pattern_utc="Tuesday 11:00 UTC",
            next_add_ts=now + (30 * 3600),
            next_remove_ts=now + (31 * 3600),
            pool_score=0.8,
        )

        selected = scanner.select_jump_now_schedules(
            schedules=[later, soon, active],
            now_ts=now,
            soon_hours=6,
            top_n=10,
        )
        self.assertEqual(len(selected), 2)
        self.assertEqual(selected[0][1], "ACTIVE NOW")
        self.assertEqual(selected[0][0].pool_id, "p1")
        self.assertEqual(selected[1][1], "STARTING SOON")
        self.assertEqual(selected[1][0].pool_id, "p2")

    def test_schedule_min_usd_per_1000_hour_overrides_quantile(self) -> None:
        start = int(dt.datetime(2025, 1, 6, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[Observation] = []
        for day in range(21):
            for hour in range(24):
                ts = start + (day * 24 + hour) * 3600
                y = 0.001  # $1.00/hr per $1k
                if (day % 7) == 1 and hour in {10, 11}:
                    y = 0.002  # $2.00/hr per $1k
                rows.append(
                    Observation(
                        source_name="s1",
                        version="v4",
                        chain="base",
                        pool_id="pool-threshold",
                        pair="AAA/BBB",
                        fee_tier=3000,
                        ts=ts,
                        volume_usd=100000.0,
                        tvl_usd=1_000_000.0,
                        fees_usd=y * 1_000_000.0,
                        hourly_yield=y,
                    )
                )

        rankings = rank_pools(rows, min_samples=24)
        end_ts = start + (21 * 24 * 3600)
        strict_schedule = scanner.build_liquidity_schedule(
            rankings=rankings,
            observations=rows,
            end_ts=end_ts,
            top_pools=1,
            quantile=0.10,  # low quantile, but should be overridden
            min_usd_per_1000_hour=1.5,
            min_hit_rate=0.90,
            min_occurrences=3,
            max_blocks_per_pool=3,
        )
        loose_schedule = scanner.build_liquidity_schedule(
            rankings=rankings,
            observations=rows,
            end_ts=end_ts,
            top_pools=1,
            quantile=0.10,
            min_usd_per_1000_hour=0.5,
            min_hit_rate=0.90,
            min_occurrences=3,
            max_blocks_per_pool=3,
        )
        self.assertTrue(strict_schedule)
        self.assertTrue(loose_schedule)
        self.assertGreaterEqual(
            strict_schedule[0].threshold_hourly_yield_pct,
            0.15,
        )

    def test_cache_upsert_and_load_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cache.sqlite"
            conn = sqlite3.connect(db_path)
            try:
                scanner.ensure_cache_schema(conn)
                rows = [
                    Observation(
                        source_name="s1",
                        version="v3",
                        chain="ethereum",
                        pool_id="pool-1",
                        pair="AAA/BBB",
                        fee_tier=3000,
                        ts=1000,
                        volume_usd=10.0,
                        tvl_usd=1000.0,
                        fees_usd=1.0,
                        hourly_yield=0.001,
                    ),
                    Observation(
                        source_name="s1",
                        version="v3",
                        chain="ethereum",
                        pool_id="pool-1",
                        pair="AAA/BBB",
                        fee_tier=3000,
                        ts=2000,
                        volume_usd=20.0,
                        tvl_usd=1000.0,
                        fees_usd=2.0,
                        hourly_yield=0.002,
                    ),
                ]
                self.assertEqual(scanner.write_observations_cache(conn, rows), 2)

                # Upsert same key with updated values.
                updated = Observation(
                    source_name="s1",
                    version="v3",
                    chain="ethereum",
                    pool_id="pool-1",
                    pair="AAA/BBB",
                    fee_tier=3000,
                    ts=1000,
                    volume_usd=11.0,
                    tvl_usd=1001.0,
                    fees_usd=1.1,
                    hourly_yield=0.0011,
                )
                scanner.write_observations_cache(conn, [updated])

                loaded = scanner.load_observations_cache(conn, start_ts=900, end_ts=2100)
                self.assertEqual(len(loaded), 2)
                self.assertEqual(loaded[0].ts, 1000)
                self.assertAlmostEqual(loaded[0].volume_usd, 11.0)
                self.assertAlmostEqual((loaded[0].hourly_yield or 0.0), 0.0011)
            finally:
                conn.close()

    def test_fetch_with_cache_uses_last_checked_for_empty_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cache.sqlite"
            conn = sqlite3.connect(db_path)
            try:
                scanner.ensure_cache_schema(conn)
                source = SourceConfig(
                    name="empty-source",
                    version="v2",
                    chain="ethereum",
                    endpoint="https://example.invalid",
                    hourly_query="query {}",
                )
                # Simulate previous successful check with no rows cached.
                scanner.upsert_source_last_checked_end_ts(conn, source, end_ts=9000)

                calls: list[tuple[int, int]] = []

                def fake_fetch(
                    source: SourceConfig,
                    start_ts: int,
                    end_ts: int,
                    page_size: int,
                    max_pages: int | None,
                    timeout: int,
                    retries: int,
                    v2_spike_min_swap_count: int,
                    v2_spike_min_reserve_weth: float,
                ) -> list[Observation]:
                    calls.append((start_ts, end_ts))
                    return []

                original = scanner.fetch_source_observations
                scanner.fetch_source_observations = fake_fetch
                try:
                    observations, failures, written = scanner.fetch_with_cache(
                        sources=[source],
                        start_ts=1000,
                        end_ts=12000,
                        page_size=100,
                        max_pages=None,
                        workers=1,
                        parallel_window_hours=24,
                        timeout=1,
                        retries=1,
                        strict_sources=False,
                        conn=conn,
                        overlap_hours=1,  # 3600s
                        checkpoint_pages=10,
                        v2_spike_min_swap_count=10,
                        v2_spike_min_reserve_weth=10.0,
                    )
                finally:
                    scanner.fetch_source_observations = original

                self.assertEqual(observations, [])
                self.assertEqual(failures, [])
                self.assertEqual(written, 0)
                self.assertEqual(len(calls), 1)
                # Should start from last_checked_end - overlap, clamped by window start/end.
                self.assertEqual(calls[0][0], 5400)
                self.assertEqual(calls[0][1], 12000)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
