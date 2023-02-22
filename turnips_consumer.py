"""
Author: Sammie Bever
Date: February 21, 2023
Class: Streaming Data
Assignment: Module 07

This program creates a consumer with 3 callbacks to go with the turnips_producer file.

To exit program, press CTRL+C.

"""
########################################################

# import python modules
import pika
import sys
import time
from collections import deque

########################################################

# define variables/constants/options
host = "localhost"
csv_file = "turnips.csv"
low_queue = "1-low-price"
medium_queue = "2-medium-price"
high_queue = "3-high-price"
show_offer = True # (RabbitMQ Server option - T=on, F=off)
sleeptime = 1

# set alert limits
low_price_alert_limit = 60 
high_price_alert_limit = 160 

########################################################

# define callback functions (called when message is received - 1 per queue)

def low_callback(ch, method, properties, body):
    """ Define behavior on getting a message in the 1-low-price queue.
    Monitor low turnip prices. Send an alert if the current price is below 60. 
    """
    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 1-low-price queue")
    # simulate work
    time.sleep(sleeptime)
    # basic_ack - acknowledge the message was received and processed (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # turnip current price/current message code
    current_price = body.decode()
    # split the current message by the delimiter ", " and put data into list form
    # the first list item [0] is our current date being read in this message
    # the second list item [1] is our current time being read in this message
    # the third list item [2] is our current price being read in this message
    turnips_current_price_split = current_price.split(", ")
    # change price to float and remove last 1 characters from string, which is the ']' character of our message
    turnips_current_price = float(turnips_current_price_split[2][:-1])

    # set up low price alert
    if turnips_current_price < low_price_alert_limit:
        print(f">>> Low price alert! Current price of turnips is below {low_price_alert_limit}.")

def medium_callback(ch, method, properties, body):
    """ Define behavior on getting a message in the 2-medium-price queue.
    Monitor medium turnip prices. 
    """
    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 2-medium-price queue")
    # simulate work
    time.sleep(sleeptime)
    # basic_ack - acknowledge the message was received and processed (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def high_callback(ch, method, properties, body):
    """ Define behavior on getting a message in the 3-high-price queue.
    Monitor high turnip prices. Send an alert if the current turnip price is above 160.
    """
    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 3-high-price queue")
    # simulate work
    time.sleep(sleeptime)
    # basic_ack - acknowledge the message was received and processed (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # turnip current price/current message code
    current_price = body.decode()
    # split the current message by the delimiter ", " and put data into list form
    # the first list item [0] is our current date being read in this message
    # the second list item [1] is our current time being read in this message
    # the third list item [2] is our current price being read in this message
    turnips_current_price_split = current_price.split(", ")
    # change price to float and remove last 1 characters from string, which is the ']' character of our message
    turnips_current_price = float(turnips_current_price_split[2][:-1])

    # set up high price alert
    if turnips_current_price > high_price_alert_limit:
        print(f">>> High price alert!  Current price of turnips is above {high_price_alert_limit}.")

########################################################

# define a main function to run the program

def main(host: str, qn: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={host}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        # need one channel per consumer
        channel = connection.channel()

        # use the channel to declare a durable queue (1 per queue)
        # a durable queue will survive a RabbitMQ server restart and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=low_queue, durable=True)
        channel.queue_declare(queue=medium_queue, durable=True)
        channel.queue_declare(queue=high_queue, durable=True)

        # The QoS level controls the # of messages that can be in-flight (unacknowledged by the consumer) at any given time.
        # Set the prefetch count to one to limit the number of messages being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # we use the auto_ack for this assignment
        channel.basic_consume(queue=low_queue, on_message_callback=low_callback, auto_ack=False)
        channel.basic_consume(queue=medium_queue, on_message_callback=medium_callback, auto_ack=False)
        channel.basic_consume(queue=high_queue, on_message_callback=high_callback, auto_ack=False)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

########################################################

# Run program

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main(host, low_queue)
    main(host, medium_queue)
    main(host, high_queue)