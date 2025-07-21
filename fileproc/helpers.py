import os, time, datetime
import threading

def log(*args):
    dt = datetime.datetime.now(datetime.timezone.utc)
    s = f"{dt}:PID {os.getpid()}:TID {threading.get_ident()}:"
    print(s, *args)

