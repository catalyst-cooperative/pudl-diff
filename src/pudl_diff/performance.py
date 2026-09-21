"""Sampling the memory and CPU use of a comparison."""

import threading
from typing import Self

import psutil


class PerformanceSampler:
    """Tracks peak whole-process RSS and CPU utilization during a `with` block.

    Polls `psutil.Process.memory_info()` and `psutil.Process.cpu_percent()`
    on a background thread. RSS sampling is used rather than
    `resource.getrusage()`'s `ru_maxrss`, since that's a lifetime
    high-water mark for the whole process (not just the block of code we care
    about) and reports in different units on macOS (bytes) than on Linux
    (KB). Sampling on an interval means a very short, sharp spike between
    samples could be missed; a shorter interval catches more spikes at the
    cost of more sampler-thread overhead.
    """

    def __init__(self, interval_seconds: float = 0.05):
        """Sample memory and CPU use every `interval_seconds`."""
        self._interval_seconds = interval_seconds
        self._process = psutil.Process()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._baseline_rss = 0
        self._peak_rss = 0
        self._peak_cpu_percent = 0.0

    def __enter__(self) -> Self:
        """Record the baseline RSS and start sampling on a background thread."""
        self._baseline_rss = self._process.memory_info().rss
        self._peak_rss = self._baseline_rss
        # cpu_percent()'s first call after a Process handle is created always
        # returns 0.0, since it has no prior timestamp to diff against; this
        # primes that reference point so the sampling loop's calls measure
        # per-interval utilization instead.
        self._process.cpu_percent()
        self._peak_cpu_percent = 0.0
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Stop sampling, after taking one last sample."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        # Catch any peak that occurred between the last sample and stopping.
        self._record_sample()

    def _sample_loop(self) -> None:
        while not self._stop_event.is_set():
            self._record_sample()
            self._stop_event.wait(self._interval_seconds)

    def _record_sample(self) -> None:
        self._peak_rss = max(self._peak_rss, self._process.memory_info().rss)
        self._peak_cpu_percent = max(
            self._peak_cpu_percent, self._process.cpu_percent()
        )

    @property
    def peak_rss_bytes(self) -> int:
        """Peak RSS observed while this sampler was running, net of the baseline."""
        return max(0, self._peak_rss - self._baseline_rss)

    @property
    def peak_cpu_percent(self) -> float:
        """Peak per-interval CPU utilization observed, as a percent of one core.

        A single-threaded process pegged at 100% shows `100.0`; a process
        using 4 cores at once can show up to `400.0`.
        """
        return self._peak_cpu_percent
