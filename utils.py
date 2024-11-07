from threading import Thread


# Runs choosen function in a new thread.
def run_thread(func):
    print("Run Thread!")
    thread = Thread(target=func)
    thread.start()


# Converts a string UNIX in milliseconds to a int UNIX in seconds.
# Example: (string) "1730969761221" --> (int) 1730969761
def unixIntSeconds(timestamp):
    new = int(float(timestamp) / 1000)
    return new
