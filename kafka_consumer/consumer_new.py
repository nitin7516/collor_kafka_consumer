import asyncio
import os
import signal
import sys
from confluent_kafka import Consumer

import consumertask
import rest

from dotenv import load_dotenv
from flask import Flask, request, jsonify

app = Flask(__name__)

cf_port = os.getenv("PORT")

@app.route("/consume")
def consume_json():
    def eventStream():
        opts = {}
        run_consumer = True
        consumer = None

        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
        opts['brokers'] = os.getenv("KAFKA_BROKERS_SASL")
        opts['rest_endpoint'] = os.getenv("KAFKA_HTTP_URL")
        opts['api_key'] = os.getenv("PASSWORD")
        opts['username'] = os.getenv("USER")
        opts['topic_name'] = os.getenv("TOPIC")

        print('Kafka Endpoints: {0}'.format(opts['brokers']))
        print('Admin REST Endpoint: {0}'.format(opts['rest_endpoint']))
        print('API_KEY: {0}'.format(opts['api_key']))

        if any(k not in opts for k in ('brokers', 'rest_endpoint', 'api_key')):
            print('Error - Failed to retrieve options. Check that app is bound to an Event Streams service or that command line options are correct.')
            sys.exit(-1)

        # Use Event Streams' REST admin API to create the topic
        # with 1 partition and a retention period of 24 hours.
        rest_client = rest.EventStreamsRest(opts['rest_endpoint'], opts['api_key'])
        print('Creating the topic {0} with Admin REST API'.format(opts['topic_name']))
        response = rest_client.create_topic(opts['topic_name'], 1, 24)
        print(response.text)

        consumer = Consumer(opts)
        
        consumer.subscribe(opts['topic_name'])
        while True:
            msg = consumer.poll(1)
            existingCoordinates = []
            if msg is not None and msg.error() is None:
                print("Message consumed new: " + str(msg.value()))
                data = json.loads(msg.value())
                print("data: " + data)
        
            yield 'data:{0}\n\n'.format(msg.value().decode())
        return Response(eventStream(), mimetype="text/event-stream") 
		



if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)
