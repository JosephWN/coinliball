import time

from coinlib.utils.threadmixin import ThreadMixin


def test_thread_mixin():
    class A(ThreadMixin):
        def __init__(self):
            self._thread_data = self.ThreadData()

        def run(self):
            while self.is_active():
                time.sleep(0.1)

    a = A()
    assert not a.is_active()
    a.start()
    assert a.is_active()
    assert not a.join(1)
    a.stop()
    assert not a.is_active()
    a.join(1)
    assert not a.thread_data.thread.is_alive()
