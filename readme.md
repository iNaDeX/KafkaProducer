Kafka Producer Class
==============

Behavior:
--------------

- Connects to Twitter Streaming API using Hosebird Client (HBC).
- Tracks tweets live, using a list of tracked terms.
- Publishes tweets into Kafka for further processing.
