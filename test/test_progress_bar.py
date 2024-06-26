import time
from dcicutils.progress_bar import ProgressBar

sleep_seconds = 0
sleep = lambda: time.sleep(sleep_seconds) if sleep_seconds > 0 else None  # noqa


def test_progress_bar_a():
    global sleep
    total = 50
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

    def run_single_task(bar: ProgressBar, total: int, task_number: int) -> None:
        global sleep
        bar.reset(total=total, progress=0, description=f"Task-{task_number}")
        for i in range(total):
            bar.increment_progress(1) ; sleep()  # noqa

    ntasks = 10
    total = 50
    description = "Working"
    bar = ProgressBar(total=total, description=description, capture_output_for_testing=True)

    for i in range(ntasks):
        run_single_task(bar, total, i + 1)
    bar.done("Done")

    # i = 0
    # for line in bar.captured_output_for_testing:
    #     print(f"{i}: {line}")
    #     i += 1

    bar_output = bar.captured_output_for_testing
    assert len(bar_output) == 1 + (ntasks * (total + 1)) + 1
    assert bar_output[0] == bar.format_captured_output_for_testing("Working", total, 0)
    assert bar_output[len(bar_output) - 1] == bar.format_captured_output_for_testing("Done", total, total)

    bar_output = bar_output[1:]
    for n in range(ntasks):
        for i in range(total + 1):
            assert bar_output[n * (total + 1) + i] == bar.format_captured_output_for_testing(f"Task-{n + 1}", total, i)
