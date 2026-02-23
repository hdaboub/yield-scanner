import csv
import datetime as dt
import tempfile
import unittest
from pathlib import Path

import backtest_lp
from scanner import Observation, ScheduleRecommendation


class BacktestTests(unittest.TestCase):
    def test_evaluate_schedules_prefers_active_window(self) -> None:
        start = int(dt.datetime(2026, 2, 2, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        rows: list[Observation] = []
        for day in range(7):
            for hour in range(24):
                ts = start + ((day * 24 + hour) * 3600)
                y = 0.0001
                if hour in {10, 11}:
                    y = 0.001
                rows.append(
                    Observation(
                        source_name="s1",
                        version="v3",
                        chain="ethereum",
                        pool_id="pool-1",
                        pair="ETH/USDC",
                        fee_tier=3000,
                        ts=ts,
                        volume_usd=100_000.0,
                        tvl_usd=1_000_000.0,
                        fees_usd=y * 1_000_000.0,
                        hourly_yield=y,
                    )
                )

        schedule = ScheduleRecommendation(
            pool_rank=1,
            source_name="s1",
            version="v3",
            chain="ethereum",
            pool_id="pool-1",
            pair="ETH/USDC",
            fee_tier=3000,
            reliability_hit_rate_pct=90.0,
            reliable_occurrences=3,
            threshold_hourly_yield_pct=0.01,
            avg_block_hourly_yield_pct=0.1,
            p90_block_hourly_yield_pct=0.1,
            block_hours=2,
            add_day_utc="Monday",
            add_hour_utc=10,
            remove_day_utc="Monday",
            remove_hour_utc=12,
            add_pattern_utc="Monday 10:00 UTC",
            remove_pattern_utc="Monday 12:00 UTC",
            next_add_ts=start,
            next_remove_ts=start + (2 * 3600),
            pool_score=1.0,
        )

        results = backtest_lp.evaluate_schedules([schedule], rows)
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertGreater(result.active_hours, 0)
        self.assertGreater(result.scheduled_avg_hourly_yield_pct, result.baseline_avg_hourly_yield_pct)
        self.assertGreater(result.uplift_vs_baseline_pct, 0)

    def test_load_observations_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hourly.csv"
            with path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "source_name",
                        "version",
                        "chain",
                        "pool_id",
                        "pair",
                        "fee_tier",
                        "timestamp",
                        "hour_utc",
                        "volume_usd",
                        "tvl_usd",
                        "fees_usd",
                        "hourly_yield_pct",
                        "annualized_apr_pct",
                    ]
                )
                writer.writerow(
                    [
                        "s1",
                        "v4",
                        "base",
                        "0xpool",
                        "WETH/USDC",
                        500,
                        1700000000,
                        "2023-11-14 22:00:00 UTC",
                        "1000",
                        "2000",
                        "1.5",
                        "0.075",
                        "",
                    ]
                )

            observations = backtest_lp.load_observations_csv(path)
            self.assertEqual(len(observations), 1)
            obs = observations[0]
            self.assertEqual(obs.pool_id, "0xpool")
            self.assertEqual(obs.pair, "WETH/USDC")
            self.assertAlmostEqual(obs.hourly_yield or 0.0, 0.00075)


if __name__ == "__main__":
    unittest.main()
