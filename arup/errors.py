#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: Errors 
  Created: 11/21/17
"""


class ArupError(Exception):
    """"""


class PublisherError(ArupError):
    """"""


class ConsumerError(ArupError):
    """"""

class MessageGetTimeoutError(ConsumerError):
    """"""

class AckError(ConsumerError):
    """"""

class NackError(ConsumerError):
    """"""
