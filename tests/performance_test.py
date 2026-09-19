"""Unit tests for pudl.validate.diff.performance."""

from pudl.validate.diff.performance import PerformanceSampler


def test_performance_sampler_tracks_peak_rss(mocker):
    """Exercise the RSS sampling logic directly against a fake memory source.

    Bypasses the background thread (never calling ``__enter__``/``__exit__``)
    so the test is deterministic instead of racing a real timer.
    """
    sampler = PerformanceSampler()
    rss_values = [1_000, 1_000, 1_500, 1_200, 1_800, 1_600]
    mocker.patch.object(
        sampler._process,
        "memory_info",
        side_effect=[mocker.Mock(rss=v) for v in rss_values],
    )
    mocker.patch.object(sampler._process, "cpu_percent", return_value=0.0)
    sampler._baseline_rss = sampler._process.memory_info().rss  # consumes 1_000
    sampler._peak_rss = sampler._baseline_rss
    for _ in range(4):
        sampler._record_sample()
    assert sampler.peak_rss_bytes == 1_800 - 1_000


def test_performance_sampler_rss_never_negative(mocker):
    """RSS dropping below the baseline shouldn't report a negative peak."""
    sampler = PerformanceSampler()
    mocker.patch.object(
        sampler._process, "memory_info", return_value=mocker.Mock(rss=500)
    )
    mocker.patch.object(sampler._process, "cpu_percent", return_value=0.0)
    sampler._baseline_rss = 1_000
    sampler._peak_rss = sampler._baseline_rss
    sampler._record_sample()
    assert sampler.peak_rss_bytes == 0


def test_performance_sampler_tracks_peak_cpu_percent(mocker):
    sampler = PerformanceSampler()
    mocker.patch.object(
        sampler._process, "memory_info", return_value=mocker.Mock(rss=1_000)
    )
    mocker.patch.object(
        sampler._process, "cpu_percent", side_effect=[50.0, 400.0, 120.0]
    )
    for _ in range(3):
        sampler._record_sample()
    assert sampler.peak_cpu_percent == 400.0


def test_performance_sampler_context_manager():
    """The real background-thread sampling loop runs without error."""
    with PerformanceSampler(interval_seconds=0.001) as sampler:
        bytearray(10_000_000)
    assert sampler.peak_rss_bytes >= 0
    assert sampler.peak_cpu_percent >= 0.0
