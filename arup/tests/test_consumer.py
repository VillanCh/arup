#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: 
  Created: 11/22/17
"""

import unittest
import time

from ..consumer import Consumer
from ..errors import MessageGetTimeoutError
from ..publisher import Publisher

class ConsumerTester(unittest.TestCase):
    """"""

    def test_consumer(self):
        """"""
        
        csm = Consumer.from_config(config_file='default.yml')
        csm.initial()
        
        with self.assertRaises(MessageGetTimeoutError):
            while True:
                frame, p, body = csm.get_message(no_ack=False, timeout=3, try_interval_ms=50)
                csm.ack(frame)
        
        
        print('building the publisher')
        pub = Publisher.from_config(config_file='default.yml')
        
        print('publisher connecting to rabbitmq')
        pub.initial()
        
        msg = 'this is a message from arup.pubulisher'
        print('publishing a message: {}'.format(msg))
        self.assertTrue(pub.publish('test', 'testkey', 'this is a message from arup.pubulisher'))
        
        print('getting a new message')
        result = csm.get_message()
        print('got a result successfully: {}!'.format(result))
        self.assertIsInstance(result, tuple)
        msg = msg.encode()
        self.assertEqual(result[2], msg)
        
        csm.ack(frame_or_dtag=result[0])
        
        pub.close()
        csm.close()
        
    
    

if __name__ == '__main__':
    unittest.main()