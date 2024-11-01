from schedule import Scheduler
from flow_demo import FlowDemo
from utils import run_thread
from time import sleep
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

schedulers: list[Scheduler] = []




def createSchedule(flow: FlowDemo):
    global schedulers
    schedule = Scheduler()

    # Creates a scheduled task that runs the function "flow.run()" every minute.
    schedule.every().minute.do(run_thread, flow.run)

    schedule.run_all()

    schedulers.append(schedule)


if __name__ == "__main__":
    print("Script Startup!")
    flow = FlowDemo("localhost", 8088)

    createSchedule(flow)

    try:
        while True:
            sleep(2)  # Sleep 2 sec
            for schedule in schedulers:
                schedule.run_pending()

    except KeyboardInterrupt as e:
        print("Keyboard Interrupt!")
    print("Script Shutdown!")
