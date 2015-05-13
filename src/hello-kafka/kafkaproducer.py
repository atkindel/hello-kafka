from kafka import SimpleProducer, KafkaClient
from random import randint

# Producer module sends out Kafka messages on port 9092
k = KafkaClient("localhost:9092")
producer = SimpleProducer(k)

# User specifies name for event log (e.g. name of module or activity)
title = str(raw_input("Name event log: "))
k.ensure_topic_exists(title)

# Unique user sends messages as needed.
uid = "Learner" + str(randint(0, 20000))
while True:
    event = raw_input("Add what to event log?: ('Q' to end.): ")
    if event == 'Q':
        break
    else:
        msg = event.encode('UTF-8', 'ignore')
        producer.send_messages(title, "%s: %s" % (uid, msg))

# Module closes connection on exit.
k.close()
