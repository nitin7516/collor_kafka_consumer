from confluent_kafka import Producer
import json

class ProducerTask(object):

    def __init__(self, conf, topic_name):
        self.topic_name = topic_name
        self.producer = Producer(conf)
        self.counter = 0
        self.running = True

    def stop(self):
        self.running = False

    def on_delivery(self, err, msg):
        if err:
            print('Delivery report: Failed sending message {0}'.format(msg.value()))
            print(err)
            # We could retry sending the message
        else:
            print('Message produced : ' + str(msg.value()))

    def run(self, geo_coordinates):
        print('The producer has started')
        key = 'key'
        try:
            self.producer.produce(self.topic_name, json.dumps(geo_coordinates), key, -1, self.on_delivery)
            self.producer.poll(0)
            self.counter += 1
        except Exception as err:
            print('Failed sending message {0}'.format(geo_coordinates))
            print(err)
        self.producer.flush()

