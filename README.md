# streaming-07-custom-project

Author: Sammie Bever
Date: February 14, 2023 
Class: Streaming Data 
Assignment: Module 07

For my custom project, I will be reading from a csv file using a producer. I will also have a consumer reading tasks from the queue.

## describe your unique steaming analytics project - what / why 
I found a dataset that looks at trading prices from a video game I enjoy playing called “Animal Crossing.” In the game, you can buy and sell turnips. If you trade strategically (ex: buy them when the price is low and sell them when the price is high), you can make a profit off of your turnips. In the game, they call it the “Stalk Market.” The dataset that I found shows the turnip price per day and time (prices were checked three times a day). I think it would be interesting to stream this dataset and be alerted when turnip prices are low (ideal for buying) and when turnip prices are high (ideal for selling). I envision setting this up similar to our module 5 & 6 smoker project where I could set up alerts for the three different price ranges: low, average, and high. 

## describe and link to your data original data sources. 
Dataset found on the following website:
Dataset is called “turnips”
https://vincentarelbundock.github.io/Rdatasets/datasets.html

Description of dataset: https://vincentarelbundock.github.io/Rdatasets/doc/stevedata/turnips.html

Link to csv:
https://vincentarelbundock.github.io/Rdatasets/csv/stevedata/turnips.csv

## describe your process - producers, consumers, exchanges, queues

## provide clickable links to the output of your simulation or process. 

# Assignment Requirements
Project: Build a unique custom streaming process based on what you've learned working with Python, pika, and RabbitMQ.

## Option 1 - Custom Project for your GitHub Repo / Portfolio
1. Describe and plan an new implementation using RabbitMQ for streaming data. 
2. Create one or more custom producers.
3. Create one or more custom consumers.
4. You can simulate your initial data source using Faker or some other file - or read from an API (not too much, too often, or too fast!)
5. How did you explore exchanges and queues? Did you use time windows?
6. What made this an interesting streaming project for you?