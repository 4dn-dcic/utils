import threading

from dcicutils.exceptions import MultiError
from dcicutils.lang_utils import n_of
from dcicutils.misc_utils import chunked, environ_bool, PRINT

# Intended use (see tests for more examples):
#
# Either of:
#
#     list(pmap(lambda x: x + 1, range(100)))
#
# or
#
#     pmap_list(lambda x: x + 1, range(100))
#
# is like
#
#     list(map(lambda x: x + 1, range(100)))
#
# except that the mapping has parallelism (chunk size 10 by default, managed by keyword arguments).
#
# To use the chunking, you might instead have written
#
#    result = []
#    for chunk in pmap_chunks(lambda x: x + 1, range(100)):
#      result += chunk
#
# Caveats:
#
#  * This does not manage timeouts or other forms of abort, abandoned threads must still run to completion,
#    so all functions mapped must arrange for their own timeouts.


class Task:

    def __init__(self, *, manager, thread=None, call_id, position, function, arg1, more_args=None):
        self.call_id = call_id
        self.position = position
        self.thread = thread
        self.function = function
        self.arg1 = arg1
        self.more_args = more_args or []
        self.ready = False
        self.result = None
        self.error = None
        self.manager: TaskManager = manager

    def __str__(self):
        return f"<{self.__class__.__name__} {self.manager} call {self.call_id} arg {self.position}>"

    def set_result(self, result):  # If this is called, it should be the last thing the tread does
        self.result = result
        self.ready = True

    def set_error(self, error):  # If this is called, it should be the last thing the thread does
        self.error = error
        self.ready = True

    def init(self):
        self.thread = threading.Thread(target=lambda x: x.call(), args=(self,))

    def start(self):
        self.thread.start()

    def call(self):
        if self.ready:
            raise RuntimeError("{self} has already been called.")
        else:
            try:
                self.set_result(self.function(self.arg1, *self.more_args))
            except Exception as e:
                self.set_error(e)

    def finish(self):
        self.thread.join()

    def kill(self):
        # We don't have a way to do this for now, so just leave it orphaned rather than blocking,
        # but this class can subclassed with something having such a method
        # if someone wants to add a method to do it.
        pass


class TaskManager:

    DEFAULT_CHUNK_SIZE = 10
    TASK_CLASS = Task
    VERBOSE = environ_bool("TASK_MANAGER_VERBOSE")
    _COUNTER_LOCK = threading.Lock()
    _ID_COUNTER = 0

    def __init__(self, fail_fast=True, raise_error=True, chunk_size=None):
        if fail_fast and not raise_error:
            raise ValueError("raise_erorr cannot be false if fail_fast is true.")
        self.fail_fast = fail_fast
        self.raise_error = raise_error
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        self.manager_id = self.new_manager_id()
        self._call_id = 0

    def __str__(self):
        return f"<{self.__class__.__name__} {self.manager_id}>"

    @classmethod
    def new_manager_id(cls):
        with cls._COUNTER_LOCK:
            cls._ID_COUNTER = id_counter = cls._ID_COUNTER + 1
            return id_counter

    def new_call_id(self):
        with self._COUNTER_LOCK:
            self._call_id = call_id = self._call_id + 1
            return call_id

    @classmethod
    def pmap(cls, fn, seq, *more_seqs, fail_fast=True, raise_error=True, chunk_size=None):
        for chunk in cls.pmap_chunks(fn, seq, *more_seqs, fail_fast=fail_fast, raise_error=raise_error,
                                     chunk_size=chunk_size):
            for item in chunk:
                yield item

    @classmethod
    def pmap_list(cls, fn, seq, *more_seqs, fail_fast=True, raise_error=True, chunk_size=None):
        result = []
        for chunk in cls.pmap_chunks(fn, seq, *more_seqs, fail_fast=fail_fast, raise_error=raise_error,
                                     chunk_size=chunk_size):
            result += chunk
        return result

    @classmethod
    def pmap_chunks(cls, fn, seq, *more_seqs, fail_fast=True, raise_error=True, chunk_size=None):  # generator
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
        return manager._pmap_chunks(fn, seq, *more_seqs)

    def _pmap_chunks(self, fn, seq1, *more_seqs):
        call_id = self.new_call_id()
        chunk_post = 0
        more_seq_generators = [(x for x in seq) for seq in more_seqs]
        for chunk in chunked(seq1, chunk_size=self.chunk_size):
            n = len(chunk)
            tasks = [Task(manager=self, call_id=call_id, position=chunk_post + i,
                          function=fn, arg1=chunk_element,
                          more_args=[next(seq_generator) for seq_generator in more_seq_generators])
                     for i, chunk_element in enumerate(chunk)]
            for task in tasks:
                task.init()
            try:
                for task in tasks:
                    if self.VERBOSE:  # pragma: no cover
                        PRINT(f"Starting {task}...")
                    task.start()
                for task in tasks:
                    if self.VERBOSE:  # pragma: no cover
                        PRINT(f"Finishing {task}...")
                    task.finish()
                    if task.error and self.fail_fast:
                        if self.VERBOSE:  # pragma: no cover
                            PRINT(f"While accumulating tasks, an error was found and is being raised due to fail_fast.")
                        raise task.error
                if not self.raise_error:
                    if self.VERBOSE:  # pragma: no cover
                        PRINT(f"Because fail_fast is false, returning list of {n_of(n, 'error or result')}"
                              f" for chunk {chunk}.")
                    yield list(map(lambda record: record.error or record.result, tasks))
                else:
                    errors = [record.error for record in tasks if record.ready and record.error]
                    if not errors:
                        if self.VERBOSE:  # pragma: no cover
                            PRINT(f"No errors to raise in chunk {chunk}.")
                        yield [record.result for record in tasks]
                    elif len(errors) == 1:
                        if self.VERBOSE:  # pragma: no cover
                            PRINT(f"Just one error to raise in chunk {chunk}.")
                        raise errors[0]
                    else:
                        if self.VERBOSE:  # pragma: no cover
                            PRINT(f"Multiple errors to raise as a MultiError in chunk {chunk}.")
                        raise MultiError(*errors)
            finally:
                for task in tasks:
                    task.kill()
            chunk_post += n


pmap_chunks = TaskManager.pmap_chunks
pmap = TaskManager.pmap
pmap_list = TaskManager.pmap_list
