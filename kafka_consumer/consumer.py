import asyncio
import os
import signal
import sys

import consumertask
import rest

from dotenv import load_dotenv
from flask import Flask, request, jsonify

app = Flask(__name__)

cf_port = os.getenv("PORT")

    @app.route("/consume")
	def consume_json():
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
        rest_client = rest.EventStreamsRest(opts['rest_endpoint'], self.opts['api_key'])
        print('Creating the topic {0} with Admin REST API'.format(self.opts['topic_name']))
        response = rest_client.create_topic(opts['topic_name'], 1, 24)
        print(response.text)

        # Use Event Streams' REST admin API to list the existing topics
        print('Admin REST Listing Topics:')
        response = rest_client.list_topics()
        print(response.text)
		
		run_tasks(opts, run_consumer, consumer)
		SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
        json_url = os.path.join(SITE_ROOT, "coordinates.json")
        data = json.load(open(json_url))
        return render_template('showjson.jade', data=data)


    async def run_tasks(opts, run_consumer, consumer):
        driver_options = {
            'bootstrap.servers': opts['brokers'],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': opts['username'],
            'sasl.password': opts['api_key'],
            'api.version.request': True,
            'broker.version.fallback': '0.10.2.1',
            'log.connection.close' : False
        }
        consumer_opts = {
            'client.id': 'kafka-python-console-sample-consumer',
            'group.id': 'kafka-python-console-sample-group'
        }

        # Add the common options to consumer and producer
        for key in driver_options:
            consumer_opts[key] = driver_options[key]

        tasks = []

        if run_consumer:
            consumer = consumertask.ConsumerTask(consumer_opts, opts['topic_name'])
			consumer.run()
            tasks.append(asyncio.ensure_future(consumer.run()))

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)
