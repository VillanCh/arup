#!/usr/bin/env python3
# coding:utf-8
import time
import unittest
from pika.adapters.blocking_connection import BlockingChannel
from .. import ArupyConsumer, Arupy

_check = {
    "a": 0,
    "b": 0,
}


class A(ArupyConsumer):
    queue_name = "test-a"

    def on_channel_created(self, channel: BlockingChannel):
        channel.queue_declare(self.queue_name)
        channel.exchange_declare("test", "direct")
        channel.queue_bind(self.queue_name, "test", "testa")

    def handle(self, channel: BlockingChannel, methods, props, body: bytes):
        # self.app.connection.sleep(2)
        print(body)
        channel.basic_ack(methods.delivery_tag)
        _check["a"] += 1
        time.sleep(3)


class B(ArupyConsumer):
    queue_name = "test-b"

    def on_channel_created(self, channel: BlockingChannel):
        channel.queue_declare(self.queue_name)
        channel.exchange_declare("test", "direct")
        channel.queue_bind(self.queue_name, "test", "testb")

    def handle(self, channel, methods, props, body: bytes):
        # self.app.connection.sleep(2)
        print(body)
        channel.basic_ack(methods.delivery_tag)
        _check["b"] += 1


class MultiConsumerCase(unittest.TestCase):

    def test_blockingtest(self):
        main = Arupy()
        main.add_consumer(A(main))
        main.add_consumer(B(main))

        main.start()
        print(main.consumers["test-a"].consumer.is_working.is_set())
        main.join(2)

        publisher = main.new_safe_publisher()
        publisher.publish_json("test", "testa", {"key": "testa"})
        publisher.publish_json("test", "testb", {"key": "testb"})

        time.sleep(1)
        self.assertGreaterEqual(_check["a"], 1)
        self.assertGreaterEqual(_check["b"], 1)

        publisher.publish_json("test", "testa", {"key": "testa"})
        publisher.publish_json("test", "testb", {"key": "testb"})

        time.sleep(1)
        print('prepare to stop')
        self.assertGreaterEqual(_check["a"], 1)
        self.assertGreaterEqual(_check["b"], 2)

        time.sleep(3)
        self.assertGreaterEqual(_check["a"], 2)

        # main.stop()
