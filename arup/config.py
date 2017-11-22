#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: 
  Created: 11/17/17
"""

import yaml
import os
import pika

_KW_CREDENTIAL = 'credential'
_KW_CONNECTION = 'connection'
_KW_CONSUMER = 'consumer'
_KW_PUBLISHER = 'publisher'
_KW_connection_retry_interval = 'connection_retry_interval'

# connection param config
#
# [connection]
# host=None,
# port=None,
# virtual_host=None,
# channel_max=None,
# frame_max=None,
# heartbeat_interval=None,
# ssl=None,
# ssl_options=None,
# connection_attempts=None,
# retry_delay=None,
# socket_timeout=None,
# locale=None,
# backpressure_detection=None
#
_DEFAULT_CONNECTION_CONFIG = {
    "host":  '127.0.0.1',
    "port":  5672,
    "virtual_host":  '/',
    "channel_max":  None,
    "frame_max":  None,
    "heartbeat_interval":  None,
    "ssl":  None,
    "ssl_options":  None,
    "connection_attempts":  None,
    "retry_delay":  None,
    "socket_timeout":  None,
    "locale":  None,
    "backpressure_detection":  None
}

_DEFAULT_CREDENTIAL_CONFIG = {
    'type': 'plain',
    'username': 'guest',
    'password': 'guest',
    'erase_on_connect': False
}

_DEFAULT_CONSUMER_CONFIG = {
    'queue': {
        'queue': 'queue-name',
        'passive': False, 
        'durable': False, 
        'exclusive': False, 
        'auto_delete': False, 
        'arguments': {}
    },
    'exchanges': {
        'default-exchange': {
            'exchange_type': 'direct', 
            'passive': False, 
            'durable': False, 
            'auto_delete': False, 
            'internal': False, 
            'arguments': {}            
        }
    },
    'binds': [
        ['default-exchange', 'default-routing-key', {}]
    ]
}

_DEFAULT_PUBLISHER_CONFIG = {
    'exchanges': {
        'default-exchange': {
            'exchange_type': 'direct', 
            'passive': False, 
            'durable': False, 
            'auto_delete': False, 
            'internal': False, 
            'arguments': {}            
        }
    },    
}

_DEFAULT_CONFIG_TABLE = {
    _KW_CONNECTION: _DEFAULT_CONNECTION_CONFIG,
    _KW_CONSUMER: _DEFAULT_CONSUMER_CONFIG,
    _KW_CREDENTIAL: _DEFAULT_CREDENTIAL_CONFIG,
    _KW_CREDENTIAL: _DEFAULT_PUBLISHER_CONFIG,
    _KW_connection_retry_interval: 1
}


def get_default_config():
    """"""
    _config = {}
    _config[_KW_CONNECTION] = dict(_DEFAULT_CONNECTION_CONFIG)
    _config[_KW_CONSUMER] = dict(_DEFAULT_CONSUMER_CONFIG)
    _config[_KW_CREDENTIAL] = dict(_DEFAULT_CREDENTIAL_CONFIG)
    _config[_KW_PUBLISHER] = dict(_DEFAULT_PUBLISHER_CONFIG)
    
    return _config
    
    

class ArupConfig(object):
    """"""

    def __init__(self, **config):
        """Constructor"""
        self.config = get_default_config()
        self.config.update(config)
    
    @classmethod
    def load_from_yaml_config(cls, config_file):
        """"""
        assert os.path.exists(config_file), '{} is not a valid config file.'.\
               format(config_file)
        
        _config = None
        with open(config_file) as fp:
            _config = yaml.load(fp)
        
        return cls(**_config)
    
    @property
    def pika_ssl_enable(self):
        """"""
        return False if 'plain' == self.config.get(_KW_CREDENTIAL).get('type') else True
    
    @property
    def pika_connection_parameters(self):
        """"""
        param = dict(self.config.get(_KW_CONNECTION))
        
        _cred = dict(self.config.get(_KW_CREDENTIAL))
        _cred.pop('type', 0)
        param['credentials'] = pika.PlainCredentials(**_cred)
        
        return pika.ConnectionParameters(**param)
    
    @property
    def pika_consumer_queue_declare(self):
        """"""
        return dict(self.config.get(_KW_CONSUMER).get('queue'))
    
    @property
    def pika_consumer_exchanges(self):
        """"""
        return dict(self.config.get(_KW_CONSUMER).get('exchanges'))
    
    @property
    def pika_consumer_binds(self):
        """"""
        return list(self.config.get(_KW_CONSUMER).get('binds'))
    
    @property
    def pika_publisher_exchanges(self):
        """"""
        return dict(self.config.get(_KW_PUBLISHER).get('exchanges'))
    
    @property
    def connection_retry_interval(self):
        """"""
        return self.config.get(_KW_connection_retry_interval, 1)
    
    def save(self, file):
        """"""
        with open(file, mode='w+') as fp:
            yaml.dump(self.config, fp)