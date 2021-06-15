from confluent_kafka import Consumer
import json
import os

class ConsumerTask(object):

    def __init__(self, conf, topic_name):
        self.consumer = Consumer(conf)
        self.topic_name = topic_name
        self.running = True

    def stop(self):
        self.running = False

    async def run(self):
        print('The consumer has started')
        self.consumer.subscribe([self.topic_name])
        while self.running:
            msg = self.consumer.poll(1)
            existingCoordinates = []
            if msg is not None and msg.error() is None:
                print("Message consumed: " + str(msg.value()))
                data = json.loads(msg.value())
                existingCoordinates.insert(0, data)
                data_json = json.dumps(existingCoordinates)
                if(os.path.isfile('coordinates.json')):
                    with open('coordinates.json', 'r+') as file:
                        file_data = json.load(file)
                        file_data.insert(0,data)
                        file.seek(0)
                        json.dump(file_data, file, indent=4)
                else:
                    with open('coordinates.json', 'w') as outfile:
                        outfile.write(data_json)
                
        self.consumer.unsubscribe()
        self.consumer.close()
