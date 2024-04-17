import time
from dcicutils.progress_bar import ProgressBar  # noqa

sleep_seconds = 0
sleep = lambda: time.sleep(sleep_seconds) if sleep_seconds > 0 else None  # noqa


def test_progress_bar_a():
    global sleep
    total = 1000
    description = "Working"
    bar = ProgressBar(total=total, description=description, capture_output_for_testing=True)

    for i in range(total):
        bar.increment_progress(1) ; sleep()  # noqa
    bar.done("Done")

    bar_output = bar.captured_output_for_testing
    # Bar output count is total plus-one for 0/total and and other plus-one for "Done" after total/total (100%).
    assert len(bar_output) == total + 2

    i = 0
    for line in bar_output:
        if i <= total:
            expected_line = bar.format_captured_output_for_testing(description, total, i)
        elif i == total + 1:
            expected_line = bar.format_captured_output_for_testing("Done", total, total)
        assert line == expected_line
        i += 1


def test_progress_bar_b():
    pass
