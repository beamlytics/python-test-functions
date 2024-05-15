import time

import schedule


def print_message():
    print("Task executed at:", time.strftime("%H:%M:%S"))


# Schedule task to run every 5 seconds
schedule.every(0.1).seconds.do(print_message)

# Keep the program running to allow scheduled tasks to execute
while True:
    schedule.run_pending()
    time.sleep(1)