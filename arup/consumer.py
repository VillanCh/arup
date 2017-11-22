#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: Consumer
  Created: 11/22/17
"""

import os
import traceback
import time
import warnings
import pika

from .config import ArupConfig
from .common import Connectable
from .errors import ConsumerError, MessageGetTimeoutError, \
     AckError, NackError

class Consumer(Connectable):
    """"""

    def __init__(self, arup_config=None):
        """Constructor"""
        self.config = arup_config
        assert isinstance(arup_config, ArupConfig)
        
        self.connection_params = self.config.pika_connection_parameters
        assert isinstance(self.connection_params, pika.ConnectionParameters), \
               'not a valid pika.ConnectionParameters instance, instead of {}'.\
               format(type(self.connection_params))
        
        self.queue_declare_params = self.config.pika_consumer_queue_declare
        assert self.queue_declare_params, 'empty queue declaring is not allowed.'
        
        # binds and exchanges declare is not neccessary,
        # because of the you can declare a queue and send message 
        # directly to the queue.
        self.binds_declares = self.config.pika_consumer_binds
        self.exchanges_declares = self.config.pika_consumer_exchanges
        
        self.queue_name = self.queue_declare_params.get('queue')
    
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
        
        self.channel.queue_declare(**self.queue_declare_params)
        
        _flag_exchange_declared = False
        for (exchange, config) in self.exchanges_declares.items():
            _flag_exchange_declared = True
            self.channel.exchange_declare(exchange=exchange, **config)
        
        queue = self.queue_name
        if _flag_exchange_declared:
            for (exchange, rkey, params) in self.binds_declares:
                self.channel.queue_bind(queue, exchange, rkey, params)
                
        return self.channel
    
    def get_message(self, timeout=5, no_ack=False, try_interval_ms=30):
        """Wrapper for pika.basic_ack(queue=self.queue_name, no_ack=False),
        When getting message, if the connection or channel is down, consumer will reconnect it.
        
        Parameters:
        ~~~~~~~~~~~
        * name: timeout 
          type: int
          desc: if consumer cannot retreive message for timeout seconds,
            the arup.errors.MessageGetTimeoutError will be raised.
        
        * name: no_ack
          type: bool
          desc: if the no_ack flag is set true, you have no need to ack the message.
        
        Returns:
        ~~~~~~~~
        a tuple contains (method_frame, properties, body).
        
        Raises:
        ~~~~~~~
        * arup.errors.MessageGetTimeoutError
          when you getting a message timeout, the current Exception will be raised.
        """
        # connection
        self.initial()
        
        endt = time.time() + timeout
        frame = None
        properties = None
        body = ''
        while time.time() <= endt:
            frame, properties, body = self.channel.basic_get(self.queue_name, no_ack)
            if frame and properties:
                break
            else:
                time.sleep(try_interval_ms/1000.0)
        
        if frame and properties:
            return frame, properties, body
        else:
            raise MessageGetTimeoutError

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
    
    def ack(self, frame_or_dtag, multiple=False):
        """"""
        dtag = self._get_delivery_tag(frame_or_dtag)
        
        if dtag is None:
            raise ValueError('the message ack error: {}'.format(frame_or_dtag))
        else:
            try:
                self.channel.basic_ack(dtag, multiple=multiple)
            except:
                msg = traceback.format_exc()
                warnings.warn(msg)                
                raise AckError('error occured when ack the message: {}'.\
                               format(frame_or_dtag))
    
    def _get_delivery_tag(self, frame_or_dtag):
        """"""
        dtag = None
        if isinstance(frame_or_dtag, int):
            dtag = frame_or_dtag
        else:
            try:
                dtag = frame_or_dtag.delivery_tag
            except:
                msg = traceback.format_exc()
                warnings.warn('getting the delivery_tag error: {}'.format(msg))
                dtag = None
        
        return dtag
    
    def nack(self, frame_or_dtag, multiple=False, requeue=True):
        """"""
        dtag = self._get_delivery_tag(frame_or_dtag)
        
        if dtag is None:
            raise ValueError('the message ack error: {}'.format(frame_or_dtag))
        else:
            try:
                self.channel.basic_nack(dtag, multiple=multiple,
                                        requeue=requeue)
            except:
                msg = traceback.format_exc()
                warnings.warn(msg)                
                raise NackError('error occured when nack the message: {}'.\
                               format(frame_or_dtag))
    