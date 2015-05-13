from kafka import SimpleConsumer, KafkaClient

# Consumer listens for Kafka messages on port 9092.
k = KafkaClient('localhost:9092')

# User provides name of event log (could be producer module name, e.g.)
title = str(raw_input("Which event log to listen to?: "))

# Consumer prints out all events logged by any producer. Times out in 3 minutes.
log = SimpleConsumer(k, group=None, topic=title, iter_timeout=180)
for event in log:
    msg = event.message.value
    print msg
