#!/usr/bin/env python
# coding:utf-8
"""
  Author: v1ll4n --<>
  Purpose: TestCase for App
  Created: 09/07/18
"""
import unittest
import logging
from pika.adapters.blocking_connection import BlockingChannel

from .. import app, outils

import time

logger = outils.get_logger('arupy')
logger.setLevel(logging.INFO)

_check = {}


class Consumer(app.ArupyConsumer):
    """"""

    queue_name = "arupy-test-app"

    def on_channel_created(self, channel: BlockingChannel):
        channel.exchange_declare("arupy", "direct")
        channel.queue_declare(queue=self.queue_name)
        channel.queue_bind(self.queue_name, "arupy", "test_app")

    def handle(self, channel: BlockingChannel, methods, props, body):
        print("got message: {}".format(body))
        _check['recv'] = True
        channel.basic_ack(methods.delivery_tag)


class ConsumerAfter(app.ArupyConsumer):
    queue_name = "arupy-test-app-for-behind-added"

    def on_channel_created(self, channel: BlockingChannel):
        _check["after-de"] = True
        channel.exchange_declare("arupy", "direct")
        channel.queue_declare(queue=self.queue_name)
        channel.queue_bind(self.queue_name, "arupy", "test_app_after")

    def handle(self, channel: BlockingChannel, methods, props, body):
        print("got message: {}".format(body))
        _check['after'] = True
        channel.basic_ack(methods.delivery_tag)


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

        time.sleep(1)
        self.assertIn("recv", _check)

        ap.add_consumer(ConsumerAfter)

        time.sleep(3)
        print("publishing the message to test_after")
        self.assertTrue(publisher.publish("arupy", "test_app_after", "this is message from safe publisher"))

        print('waiting execute.')
        time.sleep(2)
        self.assertIn("after-de", _check)
        self.assertIn("after", _check)

        print("preparing to remove Consumers")
        time.sleep(1)

        # ap.remove_consumer(ConsumerAfter.queue_name)
        # ap.remove_consumer(Consumer.queue_name)

        # ap.serve_until_no_consumers()


if __name__ == '__main__':
    unittest.main()
