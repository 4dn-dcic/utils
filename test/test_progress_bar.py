import time
from dcicutils.progress_bar import ProgressBar

sleep_seconds = 0.000001
sleep_seconds = 0.0001
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

    def run_single_task(bar: ProgressBar, total: int, task_number: int) -> None:
        bar.reset(total=total, progress=0, description=f"Task-{task_number}")
        for i in range(total):
            bar.increment_progress(1) ; sleep()  # noqa
            if (task_number == 1) and (i == (total / 4)):
                bar.set_progress(total)
                break

    global sleep
    total = 11
    description = "Working"
    ntasks = 3
    bar = ProgressBar(total=total, description=description, capture_output_for_testing=True)

    for i in range(ntasks):
        run_single_task(bar, total, i + 1)
    bar.done("Done")

    # i = 0
    # for line in bar.captured_output_for_testing:
    #     print(f"{i}: {line}")
    #     i += 1
    # return

    bar_output = bar.captured_output_for_testing
    assert len(bar_output) == 1 + (ntasks * (total + 1)) + 1
    assert bar_output[0] == bar.format_captured_output_for_testing("Working", total, 0)
    assert bar_output[len(bar_output) - 1] == bar.format_captured_output_for_testing("Done", total, total)

    bar_output = bar_output[1:]
    for n in range(ntasks):
        for i in range(total + 1):
            assert bar_output[n * (total + 1) + i] == bar.format_captured_output_for_testing(f"Task-{n + 1}", total, i)


"""
0: Working |   0% ◀|### | 0/12 | 0.0/s | 00:00 | ETA: 00:00
1: Task-1 |   0% ◀|### | 0/12 | 0.0/s | 00:00 | ETA: 00:00
2: Task-1 |   8% ◀|### | 1/12 | 0.0/s | 00:00 | ETA: 00:00
3: Task-1 |  17% ◀|### | 2/12 | 0.0/s | 00:00 | ETA: 00:00
4: Task-1 |  25% ◀|### | 3/12 | 0.0/s | 00:00 | ETA: 00:00
5: Task-1 |  33% ◀|### | 4/12 | 0.0/s | 00:00 | ETA: 00:00
6: Task-1 ✓ 100% ◀|### | 12/12 | 0.0/s | 00:00 | ETA: 00:00
7: Task-2 |   0% ◀|### | 0/12 | 0.0/s | 00:00 | ETA: 00:00
8: Task-2 |   8% ◀|### | 1/12 | 0.0/s | 00:00 | ETA: 00:00
9: Task-2 |  17% ◀|### | 2/12 | 0.0/s | 00:00 | ETA: 00:00
10: Task-2 |  25% ◀|### | 3/12 | 0.0/s | 00:00 | ETA: 00:00
11: Task-2 |  33% ◀|### | 4/12 | 0.0/s | 00:00 | ETA: 00:00
12: Task-2 |  42% ◀|### | 5/12 | 0.0/s | 00:00 | ETA: 00:00
13: Task-2 |  50% ◀|### | 6/12 | 0.0/s | 00:00 | ETA: 00:00
14: Task-2 |  58% ◀|### | 7/12 | 0.0/s | 00:00 | ETA: 00:00
15: Task-2 |  67% ◀|### | 8/12 | 0.0/s | 00:00 | ETA: 00:00
16: Task-2 |  75% ◀|### | 9/12 | 0.0/s | 00:00 | ETA: 00:00
17: Task-2 |  83% ◀|### | 10/12 | 0.0/s | 00:00 | ETA: 00:00
18: Task-2 |  92% ◀|### | 11/12 | 0.0/s | 00:00 | ETA: 00:00
19: Task-2 ✓ 100% ◀|### | 12/12 | 0.0/s | 00:00 | ETA: 00:00
20: Task-3 |   0% ◀|### | 0/12 | 0.0/s | 00:00 | ETA: 00:00
21: Task-3 |   8% ◀|### | 1/12 | 0.0/s | 00:00 | ETA: 00:00
22: Task-3 |  17% ◀|### | 2/12 | 0.0/s | 00:00 | ETA: 00:00
23: Task-3 |  25% ◀|### | 3/12 | 0.0/s | 00:00 | ETA: 00:00
24: Task-3 |  33% ◀|### | 4/12 | 0.0/s | 00:00 | ETA: 00:00
25: Task-3 |  42% ◀|### | 5/12 | 0.0/s | 00:00 | ETA: 00:00
26: Task-3 |  50% ◀|### | 6/12 | 0.0/s | 00:00 | ETA: 00:00
27: Task-3 |  58% ◀|### | 7/12 | 0.0/s | 00:00 | ETA: 00:00
28: Task-3 |  67% ◀|### | 8/12 | 0.0/s | 00:00 | ETA: 00:00
29: Task-3 |  75% ◀|### | 9/12 | 0.0/s | 00:00 | ETA: 00:00
30: Task-3 |  83% ◀|### | 10/12 | 0.0/s | 00:00 | ETA: 00:00
31: Task-3 |  92% ◀|### | 11/12 | 0.0/s | 00:00 | ETA: 00:00
32: Task-3 ✓ 100% ◀|### | 12/12 | 0.0/s | 00:00 | ETA: 00:00
33: Done ✓ 100% ◀|### | 12/12 | 0.0/s | 00:00 | ETA: 00:00
"""
