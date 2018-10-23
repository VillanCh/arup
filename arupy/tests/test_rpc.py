#!/usr/bin/env python3
# coding:utf-8
import unittest
import time

from ..app import Arupy
from ..consumers import rpc


_global_check = {}

class RPCHandler(object):

    @staticmethod
    def testme(var1, va2=1):
        print("testme is called")
        _global_check["testme"] = True
        return "test"


class RpcTest(unittest.TestCase):

    def test_rpc(self):
        app = Arupy('config.yml')
        app.add_consumer(rpc.ArupyRPCConsumer(app=app, queue_name="rpc-test", handler=RPCHandler))

        app.start()

        time.sleep(1)


        print("Consumer is started.")
        client = rpc.ArupyRPCClient(app=app, queue_name="rpc-test")

        time.sleep(1)

        result = client.call("testme", (1, "va2"))

        time.sleep(1)
        self.assertIn("testme", _global_check)
        self.assertEqual("test", result.result)

        app.stop()


if __name__ == "__main__":
    unittest.main()