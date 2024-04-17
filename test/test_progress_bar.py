from dcicutils.progress_bar import ProgressBar  # noqa

sleep_seconds = 0
sleep = lambda: time.sleep(sleep_seconds) if sleep_seconds > 0 else None  # noqa


def test_progress_bar_a():
    global sleep
    total = 1000
    description = "Working"
    _ = ProgressBar(total=total, description=description, capture_output_for_testing=True)


def test_progress_bar_b():
    pass
