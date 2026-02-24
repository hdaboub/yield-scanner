import datetime as dt
import sqlite3
import tempfile
import unittest
from pathlib import Path

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
            self.assertIn("Window UTC", text)

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
