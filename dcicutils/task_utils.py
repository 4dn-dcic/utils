import threading

from dcicutils.exceptions import MultiError
from dcicutils.lang_utils import n_of
from dcicutils.misc_utils import chunked, environ_bool, PRINT

# TODO:
#  * timeoouts
#  * does not kill threads on abort. they have to run their course.

class Task:
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

    TASK_CLASS = Task

    DEFAULT_CHUNK_SIZE = 10

    VERBOSE = environ_bool("TASK_MANAGER_VERBOSE")

    def __init__(self, fail_fast=True, raise_error=True, chunk_size=None):
        if fail_fast and not raise_error:
            raise ValueError("raise_erorr cannot be false if fail_fast is true.")
        self.fail_fast = fail_fast
        self.raise_error = raise_error
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE

    @classmethod
    def pmap(cls, fn, seq, *more_seqs, fail_fast=True, raise_error=True, chunk_size=None):
        for chunk in cls.map_chunks(fn, seq, *more_seqs, fail_fast=fail_fast, raise_error=raise_error,
                                    chunk_size=chunk_size):
            for item in chunk:
                yield item

    @classmethod
    def pmap_list(cls, fn, seq, *more_seqs, fail_fast=True, raise_error=True, chunk_size=None):
        result = []
        for chunk in cls.map_chunks(fn, seq, *more_seqs, fail_fast=fail_fast, raise_error=raise_error,
                                    chunk_size=chunk_size):
            result += chunk
        return result

    @classmethod
    def map_chunks(cls, fn, seq, *more_seqs, fail_fast=True, raise_error=True, chunk_size=None):  # generator
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
        :param chunk_size: How many items to do at once. If unsupplied, the .DEFAULT_CHUNK_SIZE will be used.
        """
        manager = TaskManager(fail_fast=fail_fast, raise_error=raise_error, chunk_size=chunk_size)
        return manager._map_chunks(fn, seq, *more_seqs)

    def _map_chunks(self, fn, seq1, *more_seqs):
        chunk_post = 0
        more_seq_generators = [(x for x in seq) for seq in more_seqs]
        for chunk in chunked(seq1, chunk_size=self.chunk_size):
            n = len(chunk)
            records = [Task(manager=self, position=chunk_post + i, function=fn, arg1=chunk_element,
                            more_args=[next(seq_generator) for seq_generator in more_seq_generators])
                       for i, chunk_element in enumerate(chunk)]
            for record in records:
                record.thread = threading.Thread(target=lambda x: x.call(), args=(record,))
            for record in records:
                if self.VERBOSE:  # pragma: no cover
                    PRINT(f"Starting {record.thread} for arg {record.position}...")
                record.thread.start()  # make sure they all start
            for record in records:
                if self.VERBOSE:  # pragma: no cover
                    PRINT(f"Joining {record.thread} for arg {record.position}...")
                record.thread.join()
                if record.error and self.fail_fast:
                    if self.VERBOSE:  # pragma: no cover
                        PRINT(f"While accumulating records, an error was found and is being raised due to fail_fast.")
                    raise record.error
            if not self.raise_error:
                if self.VERBOSE:  # pragma: no cover
                    PRINT(f"Because fail_fast is false, returning list of {n_of(n, 'error or result')}"
                          f" for chunk {chunk}.")
                yield list(map(lambda record: record.error or record.result, records))
            else:
                errors = [record.error for record in records if record.ready and record.error]
                if not errors:
                    if self.VERBOSE:  # pragma: no cover
                        PRINT(f"No errors to raise in chunk {chunk}.")
                    yield [record.result for record in records]
                elif len(errors) == 1:
                    if self.VERBOSE:  # pragma: no cover
                        PRINT(f"Just one error to raise in chunk {chunk}.")
                    raise errors[0]
                else:
                    if self.VERBOSE:  # pragma: no cover
                        PRINT(f"Multiple errors to raise as a MultiError in chunk {chunk}.")
                    raise MultiError(*errors)
            chunk_post += n


map_chunks = TaskManager.map_chunks
pmap = TaskManager.pmap
pmap_list = TaskManager.pmap_list
