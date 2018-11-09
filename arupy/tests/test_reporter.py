#!/usr/bin/env python3
# coding:utf-8
import unittest
import time
from threading import Timer
from ..app import Arupy, ArupyConsumer


class ArupyTimerDemo(Timer):

    def __init__(self, app: Arupy, interval):
        Timer.__init__(self, interval=interval, function=self._handle)

        self.app = app
        self.interval = interval

        self.publisher = app.new_safe_publisher(True)

    def _handle(self):
        while True:
            print("----")
            self.publisher.publish("", "testqueue-for-timer", "asdfasdf")
            time.sleep(self.interval)

recv = {
    "test": 0
}


class TimerRecvConsumer(ArupyConsumer):
    queue_name = "testqueue-for-timer"

    def on_channel_created(self, channel):
        channel.queue_declare(self.queue_name)

    def handle(self, channel, methods, props, body: bytes):
        channel.basic_ack(methods.delivery_tag)

        recv["test"] += 1


class ReporterTestCase(unittest.TestCase):

    def test_basic_use(self):
        app = Arupy()
        app.add_timer(ArupyTimerDemo(app, 1))
        app.add_consumer(TimerRecvConsumer)
        app.start()

        time.sleep(5)
        self.assertIn("test", recv)
        self.assertGreaterEqual(recv["test"], 3)