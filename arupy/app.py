#!/usr/bin/env python3
# coding:utf-8
import json
import os
import typing
import threading
import time
import uuid
import traceback
from pydantic import BaseModel

from eterna.recordings import ServiceRecording

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

from . import config as config_utils
from . import outils

logger = outils.get_logger('arupy')


class ArupyError(Exception):
    """"""
    pass

class ArupyOld(object):
    """"""

    def __init__(self, config="config.yml"):
        """Constructor"""
        self._mainthread: threading.Thread = None
        self.daemon = True

        if isinstance(config, str) and os.path.exists(config):
            self.pika_params = config_utils.parse_mq_parameters_from_file(config)
        elif isinstance(config, pika.ConnectionParameters):
            self.pika_params = config
        elif isinstance(config, dict):
            self.pika_params = config_utils.parse_mq_parameters_from_dict(config)
        else:
            raise ValueError("Config: {} cannot be parsed".format(config))
        self.is_working = threading.Event()
        self.consumers = {}
        self.connection = None
        self.channel = None
        self.timers = []

    def start(self):
        self._mainthread = threading.Thread(name='arupy-main', target=self.run)
        self._mainthread.daemon = True
        self._mainthread.start()

    def join(self, timeout=None):
        return self._mainthread.join(timeout)

    def _closeNclear_connNchan_whatever(self):
        try:
            if self.channel:
                self.channel.close()
        except Exception:
            pass
        finally:
            self.channel = None

        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass
        finally:
            self.connection = None

    def run(self):
        if not self.is_working.is_set():
            self.is_working.set()
        else:
            logger.warn("the Arupy is running already.")
            return

        self._closeNclear_connNchan_whatever()

        # start timers
        for timer in self.timers:
            timer.start()

        logger.info('arupy is started.')
        while self.is_working.is_set():
            # setting for connection
            logger.info("Arupy main loop is running")
            try:
                if not self.connection:
                    self.connection = pika.BlockingConnection(self.pika_params)
                    logger.info('arupy is connected to mq: {}.'.format(self.pika_params))
            except:
                logger.warn("arupy connection is failed. try 3s later")
                self.connection = None
                time.sleep(3)
                continue

            # setting for channel
            try:
                if not self.channel:
                    self.channel = self.connection.channel()
                    self.channel.basic_qos(prefetch_count=1)
                    logger.info('arupy created a channel and set the qos to 1.')
            except Exception as e:
                logger.warn("create channel failed. retry 3s later: {}".format(e))
                if self.connection:
                    try:
                        self.connection.sleep(3)
                    except:
                        self.connection = None
                        time.sleep(3)
                else:
                    time.sleep(3)
                continue

            # initial_consumer
            try:
                if self.consumers:
                    logger.info("initializing consumers")
                    self.initial_consumers()
                    logger.info('init consumers succeeded.')
            except Exception:
                logger.warn("errors in initial consumer: {}".format(
                    traceback.format_exc()
                ))
                self.connection = None
                self.channel = None
                time.sleep(3)
                # raise ArupyError()

            try:
                self.channel.start_consuming()
                time.sleep(3)
            except Exception:
                logger.warn("unexpect exit when consuming. {}".format(traceback.format_exc()))
                time.sleep(3)
                self.connection = None
                self.channel = None
                continue

        self.is_working.clear()
        logger.info("Arupy is shutdown normally.")

    def add_timer(self, timer: threading.Timer):
        if not isinstance(timer, threading.Timer):
            raise TypeError("timer is not instance of threading.Timer.")
        timer.daemon = True
        self.timers.append(timer)

    def add_consumer(self, consumer, **kwargs):
        if isinstance(consumer, ArupyConsumer):
            consumer.bind_app(app=self)
        else:
            try:
                if issubclass(consumer, ArupyConsumer):
                    consumer = consumer(app=self, **kwargs)
            except Exception:
                raise TypeError(
                    "consumer should be subclass/subinstance of ArupyConsumer but {}".format(type(consumer)))

        if consumer.queue_name not in self.consumers:
            logger.info('consumer: {} is added.'.format(consumer))
            self.consumers[consumer.queue_name] = consumer

            if self.is_working.is_set():
                logger.info("reseting Arupy...")
                self.stop()

                logger.debug("waiting for the mainthread stopped.")
                self.join()

                logger.debug("restart Arupy")
                self.start()
            else:
                logger.debug("the Arupy is not running. no need to reset Arupy.")
        else:
            raise ValueError("the queue_name: {} is existed in consumers")

    def new_publisher(self):
        return ArupyPublisher(self.pika_params)

    def new_safe_publisher(self, delivery_confirm=False):
        return ArupySafePublisher(self.pika_params, delivery_confirm)

    def initial_consumers(self):
        def _(qname, consumer):
            consumer.on_channel_created(self.channel)
            logger.info("set consumer for queue name: {} tag: {}".format(qname, qname))
            self.channel.basic_consume(consumer.handle, qname, consumer_tag=qname)

        [_(qname, consumer) for (qname, consumer) in self.consumers.items()]

    def serve_until_no_consumers(self):
        while self.consumers:
            try:
                self.connection.sleep(1)
            except:
                time.sleep(1)

    def remove_consumer(self, qname):
        self.channel.basic_cancel(consumer_tag=qname)
        logger.info("cancel consumer for queue: {}".format(qname))
        self.consumers.pop(qname, None)

    def stop(self):
        try:
            self.channel.stop_consuming()
            logger.info("stop consuming succeeded.")
        except:
            logger.info("stop consuming failed.")

        if self.is_working.isSet():
            self.is_working.clear()

        self._closeNclear_connNchan_whatever()


class ConsumerModel(BaseModel):
    consumer: typing.Any
    id: str


class Arupy(object):

    def __init__(self, config="config.yml"):
        """Constructor"""
        self._mainthread: threading.Thread = threading.Thread(target=self.run)
        self._mainthread.daemon = True

        if isinstance(config, str) and os.path.exists(config):
            self.pika_params = config_utils.parse_mq_parameters_from_file(config)
        elif isinstance(config, pika.ConnectionParameters):
            self.pika_params = config
        elif isinstance(config, dict):
            self.pika_params = config_utils.parse_mq_parameters_from_dict(config)
        else:
            raise ValueError("Config: {} cannot be parsed".format(config))
        self.is_working = threading.Event()
        self.consumers: typing.Dict[str, ConsumerModel] = {}
        self.timers: typing.List[typing.Any] = []

    def start(self):
        self._mainthread.start()

    def join(self, timeout=None):
        self._mainthread.join(timeout)

    def run(self):
        self.is_working.set()
        while self.is_working.is_set():
            self.initial_consumers()
            time.sleep(1)

    def add_consumer(self, consumer, key: str = None):
        if isinstance(consumer, ArupyConsumer):
            consumer.bind_app(app=self)
        elif issubclass(consumer, ArupyConsumer):
            consumer = consumer(app=self)
        else:
            raise TypeError(
                "consumer should be subclass/instance of ArupyConsumer. got: {}".format(
                    type(consumer)
                )
            )

        mod = ConsumerModel(
            id=key or consumer.queue_name,
            consumer=consumer
        )
        if mod.id in self.consumers:
            raise ValueError("id: {} is existed in Arupy.consumers".format(
                mod.id
            ))
        self.consumers[mod.id] = mod

    def add_timer(self, timer: threading.Timer):
        if not isinstance(timer, threading.Timer):
            raise TypeError("timer is not instance of threading.Timer.")
        timer.daemon = True
        self.timers.append(timer)

    def new_publisher(self):
        return ArupyPublisher(self.pika_params)

    def new_safe_publisher(self, delivery_confirm=False):
        return ArupySafePublisher(self.pika_params, delivery_confirm)

    def initial_consumers(self):
        for timer_base in list(self.timers):
            timer: threading.Timer = timer_base
            if not timer.is_alive():
                timer.start()
            else:
                self.timers.remove(timer)

        for consumer_base in self.consumers.values():
            consumer: ArupyConsumer = consumer_base.consumer
            if not consumer.is_working.is_set():
                consumer.start()

    def serve_until_no_consumers(self):
        while len(self.consumers) > 0:
            for consumer_base in list(self.consumers.values()):
                consumer: ArupyConsumer = consumer_base.consumer
                if not consumer.is_working:
                    self.consumers.pop(consumer_base.id, 0)
            time.sleep(1)

    def remove_consumer(self, id):
        consumer_base: ConsumerModel = self.consumers.pop(id, 0)
        if not consumer_base:
            pass

        consumer: ArupyConsumer = consumer_base.consumer
        consumer.stop()

    def stop(self):
        self.is_working.clear()

        self._mainthread.join()

        for consumer in self.consumers.values():
            ins: ArupyConsumer = consumer.consumer
            ins.stop()

        self.consumers.clear()


class ArupyConsumer(object):
    """"""

    queue_name = "default"

    def __init__(self, app: Arupy= None):
        """Constructor"""
        self.app: Arupy = app
        self.connection_params: pika.ConnectionParameters = app.pika_params
        self.connection: pika.BlockingConnection = None
        self.channel: BlockingChannel = None
        self.consuming_tag: str= str(uuid.uuid4())

        self.is_working = threading.Event()

        self._main_thread = threading.Thread(target=self._run, )
        self._main_thread.daemon = True

    def start(self):
        self._main_thread.start()

    def join(self, seconds=None):
        self._main_thread.join(timeout=seconds)

    def stop(self):
        self.is_working.clear()
        try:
            if self.channel and self.consuming_tag:
                self.channel.stop_consuming(self.consuming_tag)
        except Exception:
            pass

        # self._reset_connection_and_channel()

        self._main_thread.join()

    def _reset_connection_and_channel(self):
        try:
            if self.channel:
                self.channel.close()
        except Exception:
            pass
        finally:
            self.channel = None

        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass
        finally:
            self.connection = None

    def _run(self):
        first = True
        self.is_working.set()
        while self.is_working.is_set():
            self._reset_connection_and_channel()

            if not first:
                logger.info("retrying to connect mq.")

            try:
                self.connection = pika.BlockingConnection(self.connection_params)
            except Exception as e:
                logger.error(e)
                continue

            try:
                self.channel: BlockingChannel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=1)
            except Exception as e:
                logger.error(e)
                continue

            try:
                self.on_channel_created(channel=self.channel)
                self.channel.basic_consume(
                    self.handle, self.queue_name,
                    consumer_tag=self.consuming_tag
                )
            except Exception as e:
                logger.error(e)
                continue

            try:
                self.channel.start_consuming()
            except Exception as e:
                logger.error(e)
                continue

    def bind_app(self, app: Arupy):
        self.app = app

    def on_channel_created(self, channel: BlockingChannel):
        pass

    def handle(self, channel, methods, props, body: bytes):
        pass


class ArupyPublisher(object):
    """"""

    def __init__(self, pika_params):
        """Constructor"""
        self.params = pika_params
        self.conn = pika.BlockingConnection(self.params)
        self.chan = self.conn.channel()

    def publish(self, exchange, routing_key, body, properties=None, mandatory=False, immediate=False):
        self.chan.basic_publish(exchange, routing_key, body, properties, mandatory, immediate)

    def close(self):
        self.chan.close()
        self.conn.close()


class ArupySafePublisher(object):
    """"""

    def __init__(self, pika_params, confirm=True):
        """Constructor"""
        self.params = pika_params
        self._confirm = confirm
        self.conn: BlockingConnection = None
        self.chan: BlockingChannel = None

        # set conn N chan for self
        self._initial()

    def _initial(self):
        while True:
            try:
                self.conn = pika.BlockingConnection(self.params)
                self.chan = self.conn.channel()
                if self._confirm:
                    self.chan.confirm_delivery()
                break
            except Exception as e:
                logger.warn("publisher cannot connect to mq with config: {} for {}".format(
                    self.params, e
                ))
                logger.info("2s later retry to connect mq")
                time.sleep(2)
                continue

    def publish(self, exchange, routing_key, body, properties=None, mandatory=False, immediate=False, retry_times=5):
        for index in range(retry_times):
            try:
                return self.chan.basic_publish(exchange, routing_key, body, properties, mandatory, immediate)
            except Exception as e:
                try:
                    (code, message) = e.args
                    logger.warning("Met exception! CODE:{} Reason:{}, retry {} times 1s later..".format(
                        code, message, index + 1
                    ))
                except Exception:
                    logger.warning("Unknown amqp(rabbitmq) exception is raised: {} Type: {}".format(e, type(e)))
                    logger.exception(e)
                finally:
                    time.sleep(1)
                    self._reset_by_exception(e)
        return False

    def publish_json(self, exchange, routing_key, obj, properties=None, mandatory=False, immediate=False, retry_times=5,
                     **kwargs):
        body = json.dumps(obj, **kwargs)
        return self.publish(
            exchange, routing_key, body, properties, mandatory, immediate, retry_times
        )

    def close(self, quiet=False):
        try:
            self.chan.close()
        except Exception as e:
            if not quiet:
                logger.warning("closing channel error in SafePublisher. with {} info: {}".format(
                    type(e), e
                ))

        try:
            self.conn.close()
        except Exception as e:
            if not quiet:
                logger.warning("closing connection error in SafePublsiher. with {} info: {}".format(
                    type(e), e
                ))

        self.chan = None
        self.conn = None

    def _reset_by_exception(self, e):
        logger.warn("reset publisher by exception: {}, retry!".format(e))
        try:
            self.close(quiet=True)
        except Exception:
            pass
        finally:
            self.conn = None
            self.chan = None

        self._initial()
