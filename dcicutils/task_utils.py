import threading

from dcicutils.exceptions import MultiError
from dcicutils.misc_utils import check_true, environ_bool, PRINT

# TODO:
#  * timeoouts
#  * chunking
#  * revised error handling for chunking

PMAP_VERBOSE = environ_bool("PMAP_VERBOSE")


class _Task:
    def __init__(self, *, manager, thread=None, position, function, arg1, more_args=None):
        self.position = position
        self.thread = thread
        self.function = function
        self.arg1 = arg1
        self.more_args = more_args or []
        self.ready = False
        self.result = None
        self.error = None
        self.manager: TaskManager = manager

    def set_result(self, result):
        self.result = result
        self.ready = True

    def set_error(self, error):
        self.error = error
        self.ready = True

    def call(self):
        if self.ready:
            raise RuntimeError("{self} has already been called.")
        else:
            try:
                self.set_result(self.function(self.arg1, *self.more_args))
            except Exception as e:
                self.set_error(e)


class TaskManager:

    MAX_CONCURRENT_THREADS = 10

    def __init__(self, fail_fast=True, raise_error=True):
        if fail_fast and not raise_error:
            raise ValueError("raise_erorr cannot be false if fail_fast is true.")
        self.fail_fast = fail_fast
        self.raise_error = raise_error

    _ARG_TYPE = (list, tuple)
    _ARG_TYPE_ERROR_MESSAGE = "Each arguments to pmap must be a list or a tuple."
    _ARG_LEN_ERROR_MESSAGE = "All arguments to pmap must be of the same length."

    @classmethod
    def pmap(cls, fn, seq, *more_seqs, fail_fast=True, raise_error=True):
        """
        Maps a function across given arguments with each call performed in a separate thread.
        If errors occur, they are trapped and appropriate information is communicated to the main thread.

         * If fail_fast is False, any errors for each call are detected, not just the first error.
           Otherwise, as soon as some error is detected, it is raised.
         * If raise_error is False, the errors detected are returned rather than being raised.

        :param fn: The function to map
        :param seq: A sequence of first arguments to be given to the fn.
        :param more_seqs: Additional sequences (seq2, seq3, ...) of arguments if fn requires (arg2, arg3, ...)
        :param fail_fast: Whether to stop as soon as an error is noticed.
        :param raise_error: Whether to raise errors that are detected. If false, the error object become results.
        """
        return TaskManager(fail_fast=fail_fast, raise_error=raise_error)._do_map(fn, seq, *more_seqs)

    def _do_map(self, fn, seq1, *more_seqs):
        check_true(isinstance(seq1, self._ARG_TYPE), self._ARG_TYPE_ERROR_MESSAGE, error_class=ValueError)
        check_true(all(isinstance(seq, self._ARG_TYPE) for seq in more_seqs), self._ARG_TYPE_ERROR_MESSAGE,
                   error_class=ValueError)
        n = len(seq1)
        check_true(all(len(seq) == n for seq in more_seqs), self._ARG_LEN_ERROR_MESSAGE, error_class=ValueError)
        records = [_Task(manager=self, position=i, function=fn, arg1=seq1[i],
                         more_args=[seq[i] for seq in more_seqs])
                   for i in range(n)]
        for record in records:
            record.thread = threading.Thread(target=lambda x: x.call(), args=(record,))
        for record in records:
            if PMAP_VERBOSE:  # pragma: no cover
                PRINT(f"Starting {record.thread} for arg {record.position}...")
            record.thread.start()  # make sure they all start
        for record in records:
            if PMAP_VERBOSE:
                PRINT(f"Joining {record.thread} for arg {record.position}...")
            record.thread.join()
            if record.error and self.fail_fast:
                raise record.error
        if not self.raise_error:
            return list(map(lambda record: record.error or record.result, records))
        errors = [record.error for record in records if record.ready and record.error]
        if not errors:
            return [record.result for record in records]
        elif len(errors) == 1:
            raise errors[0]
        else:
            raise MultiError(*errors)


pmap = TaskManager.pmap
