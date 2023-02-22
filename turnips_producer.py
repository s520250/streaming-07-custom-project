"""
Author: Sammie Bever
Date: February 21, 2023
Class: Streaming Data
Assignment: Module 07

This program creates a producer and multiple task queues (RabbitMQ).
It reads data from the turnips.csv file.

"""
########################################################

# import python modules
import pika
import sys
import webbrowser
import csv
import time

########################################################

# define variables/constants/options
host = "localhost"
csv_file = "turnips.csv"
low_queue = "1-low-price"
medium_queue = "2-medium-price"
high_queue = "3-high-price"
show_offer = True # (RabbitMQ Server option - T=on, F=off)
sleeptime = 3

########################################################

# define functions
## define option to open RabbitMQ admin webpage
def offer_rabbitmq_admin_site(show_offer):
    # includes show_offer variable - turn on or off in constants listed at beginning of program
    if show_offer == True:
        """Offer to open the RabbitMQ Admin website"""
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

## define delete_queue
def delete_queue(host: str, queue_name: str):
    """
    Delete queues each time we run the program to clear out old messages.
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    ch = conn.channel()
    ch.queue_delete(queue=queue_name)

## define a message to send to queue
def publish_message_to_queue(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    ### Get a connection to RabbitMQ and create a channel
    try:
        # create a connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # declare a durable queue (will survive a RabbitMQ server restart
        # and help ensure messages are processed in order)
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue; each message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message} to {queue_name}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# define getting/reading a message from the csv file & publishing to the queue
def get_message_from_csv(input_file):
    """
    Read from csv input file. Send each row as a message to the queue.
    """ 
    # read from a csv file
    input_file = open(csv_file, "r")
    reader = csv.reader(input_file, delimiter=',')

    # Skip reading the header row of csv
    next(reader)

    for row in reader:
        # define the input strings that we want to convert into float data types
        input_string_row3 = row[3]

        # Convert strings to float types
        float_row3 = float(input_string_row3)

        # turn column values into fstrings
        fstring_date = f"{row[1]}"
        fstring_time = f"{row[2]}"
        fstring_price = f"{row[3]}"

        # use an fstring to create messages from our data
        fstring_message = f"[{fstring_date}, {fstring_time}, {fstring_price}]"

        # prepare a binary (1s and 0s) message to stream
        # be careful: these are case sensitive!
        message_price = fstring_message.encode()

        # publish to queues using routing
        if float_row3 <= 80: publish_message_to_queue(host, low_queue, message_price)
        elif float_row3 > 130: publish_message_to_queue(host, high_queue, message_price)
        else: publish_message_to_queue(host, medium_queue, message_price)

        # slowly read a row per sleeptime defined in constants at beginning of program (to simulate work)
        time.sleep(sleeptime)        

########################################################

# Run program
if __name__ == "__main__":  
    # if show_offer = True, ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site(show_offer)
    # delete queues to clear old messages
    delete_queue(host, low_queue)
    delete_queue(host, medium_queue)
    delete_queue(host, high_queue)
    # get the message from the csv input file and send to queue
    get_message_from_csv(csv_file)
