import os
import sys
import thread
import threading
import time

if sys.platform == 'cli':
    import math
    import System.Threading

    class Event(object):
        __fields__ = ['event_handle']

        def __init__(self):
            self.event_handle = System.Threading.EventWaitHandle(False, System.Threading.EventResetMode.ManualReset)

        def isSet(self):
            return self.event_handle.WaitOne(0, False)

        def set(self):
            self.event_handle.Set()

        def clear(self):
            self.event_handle.Reset()

        def wait(self, timeout = None):
            if timeout is None:
                self.event_handle.WaitOne(System.Threading.Timeout.Infinite, False)
            else:
                self.event_handle.WaitOne(min(int(math.ceil(timeout * 1000)), 0xfffffffe), False)

elif os.name == 'nt':
    import math
    import ctypes
    kernel32 = ctypes.windll.kernel32
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_int, ctypes.c_uint]
    
    class Event(object):
        __fields__ = ['event_handle']
        
        _closehandle = kernel32.CloseHandle # apparently I can't expect this to exist while __del__ is being called
        
        def __init__(self):
            self.event_handle = kernel32.CreateEventA(0, 1, 0, 0)
            if not self.event_handle:
                raise ctypes.WinError()
        
        def isSet(self):
            return kernel32.WaitForSingleObject(self.event_handle, 0) == 0
        
        def set(self):
            if not kernel32.SetEvent(self.event_handle):
                raise ctypes.WinError()
        
        def clear(self):
            if not kernel32.ResetEvent(self.event_handle):
                raise ctypes.WinError()
        
        def wait(self, timeout=None):
            if timeout is None:
                kernel32.WaitForSingleObject(self.event_handle, 0xffffffff)
            else:
                kernel32.WaitForSingleObject(self.event_handle, min(int(math.ceil(timeout * 1000)), 0xfffffffe))
        
        def __del__(self):
            self._closehandle(self.event_handle)
else:
    import select
    
    class Event(object):
        __fields__ = ['r', 'w', 'lock']
        
        def __init__(self):
            self.r, self.w = os.pipe()
            self.lock = thread.allocate_lock()
        
        def isSet(self):
            readable, writeable, err = select.select([self.r], [], [], 0)
            return bool(readable)
        
        def set(self):
            if not self.isSet(): #not strictly necessary, but we'd like to keep the number of unread bytes small
                os.write(self.w, 'a')
        
        def clear(self):
            self.lock.acquire()
            while self.isSet():
                os.read(self.r, 1)
            self.lock.release()
        
        def wait(self, timeout=None):
            if timeout is None:
                select.select([self.r], [], [])
            else:
                select.select([self.r], [], [], timeout)
        
        def __del__(self):
            os.close(self.r)
            os.close(self.w)

def do_nothing(*args, **kwargs):
    pass

class Job(object):
    def __init__(self, f, args=(), kwargs={}, cb=do_nothing):
        self.exception = None
        self.result = None
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.finished = False
        self.cb = cb

class WorkerThread(threading.Thread):
    def __init__(self, threadpool):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadpool = threadpool
        self.job = None
        self.event = Event() #signaled when we have a job, or we're finished
        self.done = False

    def run(self):
        while True:
            self.event.clear()
            if self.done:
                return
            if self.job is not None:
                try:
                    self.job.result = self.job.f(*self.job.args, **self.job.kwargs)
                except BaseException, e:
                    self.job.exception = e
                finally:
                    self.job.finished = True
                    self.job = None
                    self.threadpool.signal()
            self.event.wait()

class ThreadPool(object):
    def __init__(self):
        # It's expected that all functions can be called from only one thread,
        # except when otherwise specified.
        self.event = Event()
        self.threads = []
        self.jobs = []

    def signal(self):
        # should be called only from worker threads, to indicate a job is finished.
        self.event.set()

    def refresh(self):
        for i in range(len(self.jobs)-1, -1, -1):
            job = self.jobs[i]
            if job.finished:
                job.cb(job)
                self.jobs.pop(i)

    def queue_job(self, job):
        self.refresh()

        self.jobs.append(job)

        for thread in self.threads:
            if thread.job is None:
                break
        else:
            thread = WorkerThread(self)
            self.threads.append(thread)
            thread.start()

        thread.job = job
        thread.event.set()

    def wait_for_job(self, job, timeout = None):
        self.event.clear()
        self.refresh()
        if timeout is not None:
            end_time = time.time() + timeout
        while job in self.jobs:
            if timeout is not None:
                delay = end_time - time.time()
                if delay <= 0:
                    break
            else:
                delay = None
            self.event.wait(delay)

            self.event.clear()
            self.refresh()

