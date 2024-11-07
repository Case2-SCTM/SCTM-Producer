from schedule import Scheduler
from flow_demo import FlowDemo
from utils import run_thread
from time import sleep
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Create a list, to hold our schedulers
schedulers: list[Scheduler] = []


# Creates a new scheduler that runs a function every minute, and append it to schedulers
def createSchedule(flow: FlowDemo):
    global schedulers
    schedule = Scheduler()

    # Creates a scheduled task that runs the function "flow.run()" every minute.
    schedule.every().minute.do(run_thread, flow.run)

    # Runs all scheduled task now
    schedule.run_all()

    # Append scheduler to the list schedulers
    schedulers.append(schedule)


# Only runs if main.py script is started
if __name__ == "__main__":
    print("Script Startup!")

    # Creates a object of our FlowDemo class, with ip and port to where our flow demo is running
    flow = FlowDemo("localhost", 8088)

    # Runs our createSchedule Function and creates a scheduler with the flow object
    createSchedule(flow)

    try:
        # While loop to keep script running
        while True:
            sleep(2)  # Sleep 2 sec
            for schedule in schedulers:
                # Runs any pending scheduled tasks
                schedule.run_pending()

    # A keyboard interrupt to stop the script
    # Scheduled tasks is running on another thread, so any running task will finish its task before stopping
    except KeyboardInterrupt as e:
        print("Keyboard Interrupt!")
    print("Script Shutdown!")
