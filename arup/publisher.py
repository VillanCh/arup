#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: Publisher
  Created: 11/21/17
"""

import os
import traceback
import time
import warnings
import pika

from .config import ArupConfig
from .common import Connectable
from .errors import PublisherError

class Publisher(Connectable):
    """"""

    def __init__(self, arup_config=None):
        """Constructor"""
        self.config = arup_config
        assert isinstance(arup_config, ArupConfig)
        
        # param checking
        self.connection_params = self.config.pika_connection_parameters
        if not self.connection_params:
            raise PublisherError('not a valid connection params.')
        
        self.exchange_declare = self.config.pika_publisher_exchanges
        if not self.exchange_declare:
            raise PublisherError('config don\'t have exchanges to declare.')
    
    def initial(self):
        """"""
        if not hasattr(self, 'connection') or \
           not self.connection.is_open:
            while True:
                try:
                    self.connect(self.connection_params)
                    break
                except:        
                    time.sleep(seconds=self.config.connection_retry_interval)
    
        if not hasattr(self, 'channel') or \
           not self.channel.is_open:
            while True:
                try:
                    self._create_channel()
                    break
                except:
                    time.sleep(seconds=self.config.connection_retry_interval)        

    
    @classmethod
    def from_config(cls, config_file='default.yml'):
        """"""
        assert os.path.exists(config_file), 'not a existed file: {}'.\
               format(config_file)
        
        config = ArupConfig.load_from_yaml_config(config_file)
        
        return cls(config)
    
    def _create_channel(self):
        """"""
        while True:
            try:
                self.connect(self.connection_params)
                break
            except:
                time.sleep(self.config.connection_retry_interval)
        
        self.channel = self.connection.channel()
        
        for (exchange, config) in self.exchange_declare.items():
            self.channel.exchange_declare(exchange=exchange, **config)
        
        self.channel.confirm_delivery()
        
        return self.channel
                
    def publish(self, exchange, routing_key, message, strict_mode=True, 
                max_try=5, **properties):
        """"""
        count = 0
        result = False
        while not result:
            count += 1
            if count < max_try:
                try:
                    result = self._publish(exchange, routing_key, message, 
                                           **properties)
                except Exception as e:
                    msg = traceback.format_exc()
                    warnings.warn(message=msg)
            else:
                break
        
        if strict_mode:
            if not result:
                raise PublisherError('publish message: {} failed.'.format(message))
            else:
                return result
        else:
            return result
    
    def _publish(self, exchange, routing_key, message, **properties):
        """"""
        if (not hasattr(self, 'channel')) or (not self.channel.is_open):
            while True:
                try:
                    self._create_channel()
                    if self.channel.is_open:
                        break
                    else:
                        pass
                except Exception as e:
                    msg = traceback.format_exc()
                    warnings.warn(message=msg)
                
                time.sleep(self.config.connection_retry_interval)
        
        prop = pika.BasicProperties(**properties)
        result = self.channel.basic_publish(exchange, routing_key, message,
                                            properties=prop)
        
        return result
    
    def close(self):
        """"""
        try:
            self.channel.close()
        except Exception as e:
            msg = traceback.format_exc()
            warnings.warn('when closing channel an exception is occured: {}'.format(msg))
        
        try:
            self.connection.close()
        except Exception as e:
            msg = traceback.format_exc()
            warnings.warn('when closing connection an exception is occured: {}'.format(msg))        