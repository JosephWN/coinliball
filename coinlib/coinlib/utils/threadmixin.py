import threading


class ThreadMixin:
    """Thread running while is_active() return True"""

    daemon = True

    class ThreadData:
        """self._thread_data = ThreadData()"""

        def __init__(self):
            self.is_active = threading.Event()
            self.thread: threading.Thread = None
            self.lock = threading.RLock()

    @property
    def thread_data(self) -> ThreadData:
        return getattr(self, '_thread_data')

    def is_active(self) -> bool:
        return self.thread_data.is_active.is_set()

    def _activate(self):
        self.thread_data.is_active.set()

    def _deactivate(self):
        self.thread_data.is_active.clear()

    def _wait_activation(self, timeout: float = None):
        self.thread_data.is_active.wait(timeout=timeout)

    def run(self):
        pass

    def start(self):
        def run():
            try:
                self._activate()
                self.run()
            finally:
                self._deactivate()

        with self.thread_data.lock:
            assert not self.thread_data.thread or not self.thread_data.thread.is_alive(), \
                'thread still running. use stop() and join()'
            self.thread_data.thread = threading.Thread(target=run, daemon=self.daemon)
            self.thread_data.thread.start()
            self._wait_activation(timeout=1)

    def stop(self):
        self._deactivate()

    def join(self, timeout: float = None):
        with self.thread_data.lock:
            assert self.thread_data.thread, 'thread not started'
            self.thread_data.thread.join(timeout=timeout)
