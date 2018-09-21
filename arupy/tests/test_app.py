#!/usr/bin/env python
# coding:utf-8
"""
  Author: v1ll4n --<>
  Purpose: TestCase for App
  Created: 09/07/18
"""
import unittest
import logging
import pika
from pika.adapters.blocking_connection import BlockingChannel

from .. import app, outils

import time

logger = outils.get_logger('arupy')
logger.setLevel(logging.INFO)


class Consumer(app.ArupyConsumer):
    """"""

    queue_name = "arupy-test-app"

    def on_channel_created(self, channel: BlockingChannel):
        channel.exchange_declare("arupy", "direct")
        channel.queue_declare(queue=self.queue_name)
        channel.queue_bind(self.queue_name, "arupy", "test_app")

    def handle(self, channel: BlockingChannel, methods, props, body):
        print("got message: {}".format(body))
        channel.basic_ack(methods.delivery_tag)
        self.app.remove_consumer(self.queue_name)


class AppTestCase(unittest.TestCase):
    """"""

    def test_app(self):
        ap = app.Arupy()
        ap.add_consumer(Consumer)
        ap.start()

        time.sleep(3)

        publisher = ap.new_publisher()
        publisher.publish(exchange="arupy", routing_key="test_app", body="this is message from publisher")

        publisher = ap.new_safe_publisher(True)
        self.assertTrue(publisher.publish("arupy", "test_app", "this is message from safe publisher"))

        ap.serve_until_no_consumers()




if __name__ == '__main__':

    unittest.main()
