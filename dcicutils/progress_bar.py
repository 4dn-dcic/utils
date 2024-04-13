from collections import namedtuple
from signal import signal, SIGINT
import sys
import threading
import time
from tqdm import tqdm
from types import FrameType as frame
from typing import Callable, List, Optional, Union
from contextlib import contextmanager
from dcicutils.command_utils import yes_or_no


class TQDM(tqdm):

    """
    def moveto(self, n):
      # Hack/workaround for apparent tqdm bug where (for example) if we use this twice
      # in a row, and we interrupt (CTRL-C) the first one and abort the task, then the
      # output for the second usage gets a bit garble; on the wrong line and whatnot;
      # somehow, state is stuck across usages; can't quite see how from the tqdm code.
      # This is a bit worrying but so far no other deleterious effects observed.
      # This looks maybe promising:
    return
    """

    # Nevermind the above; found a more pointed solution from here:
    # https://stackoverflow.com/questions/41707229/why-is-tqdm-printing-to-a-newline-instead-of-updating-the-same-line
    # Why in the world would tqdm be maintaining state across instances?? Whatever, this fixes it.
    def __init__(self, *args, **kwargs):
        super()._instances.clear() if super()._instances else None
        super().__init__(*args, **kwargs)


# Wrapper around tqdm command-line progress bar.
class ProgressBar:

    @staticmethod
    @contextmanager
    def define(*args, **kwargs):
        progress_bar = None
        try:
            progress_bar = ProgressBar(*args, **kwargs)
            yield progress_bar
        finally:
            if progress_bar:
                progress_bar.done()

    def __init__(self, total: Optional[int] = None,
                 description: Optional[str] = None,
                 catch_interrupt: bool = True,
                 interrupt: Optional[Callable] = None,
                 interrupt_continue: Optional[Callable] = None,
                 interrupt_stop: Optional[Callable] = None,
                 interrupt_exit: bool = False,
                 interrupt_exit_message: Optional[Union[Callable, str]] = None,
                 interrupt_message: Optional[str] = None,
                 tidy_output_hack: bool = True,
                 capture_output_for_testing: bool = False) -> None:
        self._bar = None
        self._disabled = False
        self._done = False
        self._tidy_output_hack = (tidy_output_hack is True)
        self._started = time.time()
        self._stop_requested = False
        # Interrupt handling. We do not do the actual (signal) interrupt setup
        # in self._initialize as that could be called from a (sub) thread; and in
        # Python we can only set a signal (SIGINT in our case) on the main thread.
        self._catch_interrupt = (catch_interrupt is True)
        self._interrupt = interrupt if callable(interrupt) else None
        self._interrupt_continue = interrupt_continue if callable(interrupt_continue) else None
        self._interrupt_stop = interrupt_stop if callable(interrupt_stop) else None
        if interrupt_exit in [True, False]:
            if not self._interrupt_stop:
                self._interrupt_stop = lambda _: interrupt_exit
            self._interrupt_exit = interrupt_exit
        else:
            self._interrupt_exit = None
        self._interrupt_message = interrupt_message if isinstance(interrupt_message, str) else None
        if isinstance(interrupt_exit_message, str):
            self._interrupt_exit_message = lambda bar: interrupt_exit_message
        elif isinstance(interrupt_exit_message, Callable):
            self._interrupt_exit_message = interrupt_exit_message
        else:
            self._interrupt_exit_message = None
        self._interrupt_handler = None
        if self._catch_interrupt:
            self._interrupt_handler = self._define_interrupt_handler()
        if self._tidy_output_hack is True:
            self._tidy_output_hack = self._define_tidy_output_hack()
        self._total = total if isinstance(total, int) and total >= 0 else 0
        self._description = self._format_description(description)
        self._captured_output_for_testing = [] if capture_output_for_testing else None

    def _initialize(self) -> bool:
        # Do not actually create the tqdm object unless/until we have a positive total.
        if (self._bar is None) and (self._total > 0):
            bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} | {rate_fmt} | {elapsed}{postfix} | ETA: {remaining} "
            self._bar = TQDM(total=self._total, desc=self._description,
                             dynamic_ncols=True, bar_format=bar_format, unit="", file=sys.stdout)
            if self._disabled:
                self._bar.disable = True
            return True
        return False

    def set_total(self, value: int, reset_eta: bool = False) -> None:
        if value == self._total:
            # If the total has not changed since last set then do nothing.
            if reset_eta and self._bar is not None:
                self._bar.reset()
            return
        if isinstance(value, int) and value > 0:
            self._total = value
            if self._bar is not None:
                # This reset is needed to get the ETA to reset properly when we reset
                # the total during the course of a single ProgressBar instance.
                self._bar.reset()
                self._bar.total = value
                self._bar.refresh()

    def reset_eta(self) -> None:
        # Since set_total does nothing if total is the same, provide
        # a way to reset the ETA if starting over with the same total.
        if self._bar is not None:
            progress = self._bar.n
            self._bar.reset()
            self._bar.total = self._total
            self._bar.n = progress
            self._bar.refresh()

    def set_progress(self, value: int) -> None:
        if isinstance(value, int) and value >= 0:
            if (self._bar is not None) or self._initialize():
                self._bar.n = value
                self._bar.refresh()

    def increment_progress(self, value: int) -> None:
        if isinstance(value, int) and value > 0:
            if (self._bar is not None) or self._initialize():
                self._bar.update(value)
                self._bar.refresh()

    def set_description(self, value: str) -> None:
        self._description = self._format_description(value)
        if self._bar is not None:
            self._bar.set_description(self._description)

    def done(self) -> None:
        if self._done or self._bar is None:
            return
        self._ended = time.time()
        self.set_progress(self.total)
        self._bar.set_description(self._description)
        self._bar.refresh()
        # FYI: Do NOT do a bar.disable = True before a bar.close() or it messes up output
        # on multiple calls; found out the hard way; a couple hours will never get back :-/
        self._bar.close()
        if self._tidy_output_hack:
            self._tidy_output_hack.restore()
        if self._interrupt_handler:
            self._interrupt_handler.restore()
        self._done = True

    def disable(self, value: bool = True) -> None:
        self._disabled = (value is True)
        if self._bar is not None:
            self._bar.disable = self._disabled

    def enable(self, value: bool = True) -> None:
        self.disable(not value)

    @property
    def total(self) -> int:
        return self._bar.total if self._bar else 0

    @property
    def progress(self) -> int:
        return self._bar.n if self._bar else 0

    @property
    def disabled(self) -> bool:
        return self._disabled

    @property
    def enabled(self) -> bool:
        return not self.disabled

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested

    @property
    def started(self) -> None:
        return self._started

    @property
    def duration(self) -> None:
        return time.time() - self._started

    @property
    def captured_output_for_testing(self) -> Optional[List[str]]:
        return self._captured_output_for_testing

    def _format_description(self, value: str) -> str:
        if not isinstance(value, str):
            value = ""
        if self._tidy_output_hack and not value.endswith(self._tidy_output_hack.sentinel):
            value += self._tidy_output_hack.sentinel
        return value

    def _define_interrupt_handler(self) -> None:
        def handle_interrupt(signum: int, frame: frame) -> None:  # noqa
            nonlocal self
            def handle_secondary_interrupt(signum: int, frame: frame) -> None:  # noqa
                print("\nEnter 'yes' or 'no' or CTRL-\\ to completely abort ...")
            self.disable()
            self._interrupt(self) if self._interrupt else None
            set_interrupt_handler(handle_secondary_interrupt)
            if yes_or_no(f"\nALERT! You have interrupted this {self._interrupt_message or 'process'}."
                         f" Do you want to stop{' (exit)' if self._interrupt_exit else ''}?"):
                # Here there was an interrupt (CTRL-C) and the user confirmed (yes)
                # that they want to stop the process; if the interrupt_stop handler
                # is defined and returns True, then we exit the entire process here,
                # rather than simply returning, which is the default.
                if self._interrupt_stop:
                    interrupt_stop = self._interrupt_stop(self)
                    if (interrupt_stop is True) or ((interrupt_stop is None) and (self._interrupt_exit is True)):
                        self.done()
                        restore_interrupt_handler()
                        if self._interrupt_exit_message:
                            if isinstance(interrupt_exit_message := self._interrupt_exit_message(self), str):
                                print(interrupt_exit_message)
                        exit(1)
                    elif interrupt_stop is False or ((interrupt_stop is None) and (self._interrupt_exit is False)):
                        set_interrupt_handler(handle_interrupt)
                        interrupt_continue = self._interrupt_continue(self) if self._interrupt_continue else None
                        if not (interrupt_continue is False):
                            self.enable()
                        return
                self._stop_requested = True
                return
            set_interrupt_handler(handle_interrupt)
            self._interrupt_continue(self) if self._interrupt_continue else None
            self.enable()
        def restore_interrupt_handler() -> None:  # noqa
            nonlocal self, previous_interrupt_handler
            set_interrupt_handler(previous_interrupt_handler)
        def set_interrupt_handler(interrupt_handler: Callable) -> Optional[Callable]:  # noqa
            nonlocal self
            if callable(interrupt_handler) and (threading.current_thread() == threading.main_thread()):
                return signal(SIGINT, interrupt_handler)
            return None
        previous_interrupt_handler = set_interrupt_handler(handle_interrupt)
        return namedtuple("interrupt_handler", ["restore"])(restore_interrupt_handler)

    def _define_tidy_output_hack(self) -> None:
        # Some minor tqdm output tidying-up which for annoying anomalies; tqdm forces
        # a colon (:) before the percentage, e.g. ":  25%|"; and while we're at it do
        # a little ASCII progress animation, requiring a special ([progress]) sentinal
        # string in the display string where the progress bar should actually go,
        # which we do in _format_description. Other minor things too; see below.
        sys_stdout_write = sys.stdout.write
        total_most_recent = None
        progress_most_recent = None
        description_most_recent = None
        def tidy_stdout_write(text: str) -> None:  # noqa
            nonlocal self, sys_stdout_write, sentinel_internal, spina, spini, spinn
            nonlocal total_most_recent, progress_most_recent, description_most_recent
            def replace_first(value: str, match: str, replacement: str) -> str:  # noqa
                return value[:i] + replacement + value[i + len(match):] if (i := value.find(match)) >= 0 else value
            def remove_extra_trailing_spaces(text: str) -> str:  # noqa
                while text.endswith("  "):
                    text = text[:-1]
                return text
            if not text or not self._bar:
                return
            if (self._bar.total == total_most_recent) and (self._bar.n == progress_most_recent):
                return
            total_most_recent = self._bar.total
            progress_most_recent = self._bar.n
            description_most_recent = self._description
            if (self._disabled or self._done) and sentinel_internal in text:
                # Another hack to really disable output on interrupt; in this case we set
                # tqdm.disable to True, but output can still dribble out, so if the output
                # looks like it is from tqdm and we are disabled/done then do no output.
                return
            if sentinel_internal in text:
                spinc = spina[spini % spinn] if not ("100%|" in text) else "| ✓" ; spini += 1  # noqa
                text = replace_first(text, sentinel_internal, f" {spinc}")
                text = replace_first(text, "%|", "% ◀|")
                text = remove_extra_trailing_spaces(text)
                # Another oddity: for the rate sometimes tqdm intermittently prints
                # something like "1.54s/" rather than "1.54/s"; something to do with
                # the unit we gave, which is empty; idunno; just replace it here.
                text = replace_first(text, "s/ ", "/s ")
            sys_stdout_write(text)
            if self._captured_output_for_testing is not None:
                self._captured_output_for_testing.append(text)
            sys.stdout.flush()
        def restore_stdout_write() -> None:  # noqa
            nonlocal sys_stdout_write
            if sys_stdout_write is not None:
                sys.stdout.write = sys_stdout_write
        sys.stdout.write = tidy_stdout_write
        spina = ["|", "/", "—", "◦", "\\"] ; spini = 0 ; spinn = len(spina)  # noqa
        sentinel = "[progress]" ; sentinel_internal = f"{sentinel}:"  # noqa
        return namedtuple("tidy_output_hack", ["restore", "sentinel"])(restore_stdout_write, sentinel)
