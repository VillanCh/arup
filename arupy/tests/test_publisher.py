#!/usr/bin/env python3
# coding:utf-8
from ..app import Arupy
import unittest
import time

class PublisherTestCase(unittest.TestCase):

    def test_publish_error_test(self):
        app = Arupy()

        print("create publsiher")
        publisher = app.new_safe_publisher(True)

        # non-existed exchange will raise 404.
        start = time.time()
        publisher.publish("asdasdfasdfaqwerqwerqwer23", "asdfasdf", "body", retry_times=3)
        end = time.time()

        self.assertTrue(end - start >= 3)

        # publish to queue is never failed. (emtpy for exchange, queue name for routing_key.)
        result = publisher.publish("", "joasdfasdfasdfnosc", "asdfasdfasdfasdf", retry_times=2)
        self.assertTrue(result)

        self.assertTrue(publisher.publish_json("", "asdfasdf", {"tasdfasdfaadsasdfasdf": "asdfasdf"}))

        time.sleep(1)

        publisher.close()

if __name__ == "__main__":
    unittest.main()