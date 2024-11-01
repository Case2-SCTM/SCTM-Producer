from threading import Thread


def run_thread(func):
    print("Run Thread!")
    thread = Thread(target=func)
    thread.start()


def unixIntSeconds(timestamp):
    new = int(float(timestamp) / 1000)
    return new
