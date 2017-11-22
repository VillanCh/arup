#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: 
  Created: 11/21/17
"""

import unittest

from ..publisher import Publisher

class PublishTester(unittest.TestCase):
    """"""

    def test_publish(self):
        """"""
        pub = Publisher.from_config(config_file='default.yml') 
        pub.initial()
        properties = {}
        exchange = 'test'
        routing_key = 'test'
        message = 'this is a test message from arup publisher'
        result = pub.publish(exchange, routing_key, message, **properties)
        
        pub.close()

    
    

if __name__ == '__main__':
    unittest.main()