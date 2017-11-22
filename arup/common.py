#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: Common
  Created: 11/21/17
"""

import traceback
from pika import BlockingConnection, ConnectionParameters

from .config import ArupConfig

class Connectable(object):
    """"""
    
    def _connect(self, connection_params):
        """
        Connect using pika (should be used by self.connect)
        
        Return:
        ~~~~~~~
          Return the instance of pika.BlockingConnection
        
        Raise:
        ~~~~~~
          If the connection is failed. pika will raise the exception.
        So the self._connect is also to raise the exception in pika
          
        """
        return BlockingConnection(connection_params)

    def connect(self, connection_params=None):
        """
        Connect to RabbitMQ using pika.BlockingConnection
        
        Parameters:
        ~~~~~~~~~~~
        connection_params: :pika.ConnectionParameters:
          connection params for pika
        
        """
        cparams = connection_params
        assert isinstance(cparams, ConnectionParameters), 'no a valid ' + \
               'ConnectionParameters instance.'
        
        connection = getattr(self, 'connection', 0)
        if not connection:
            self.connection = self._connect(cparams)
        else:
            assert isinstance(self.connection, BlockingConnection)
            
            if self.connection.is_open:
                pass
            else:
                self.connection = self._connect(cparams)
        
        return self.connection
    
    def _create_channel(self):
        """"""
        raise NotImplemented()
            