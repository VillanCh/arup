#!/usr/bin/env python3
# coding:utf-8
import json
import uuid
import pika
import threading
import queue
import traceback
from pika.adapters.blocking_connection import BlockingChannel
from ..app import Arupy, ArupyConsumer, ArupySafePublisher
from ..outils import get_logger

logger = get_logger("arupy")


class RPCRequest(object):

    def __init__(self, target, vargs=None, kwargs=None, trace_id=None):
        self._data = {
            "target": target,
            "vargs": vargs or (),
            "kwargs": kwargs or {},
            "trace_id": trace_id or uuid.uuid4().hex
        }

    @property
    def target(self):
        return self._data["target"]

    @property
    def vargs(self):
        return self._data["vargs"]

    @property
    def kwargs(self):
        return self._data["kwargs"]

    @property
    def trace_id(self):
        return self._data["trace_id"]

    def to_json(self):
        return json.dumps(self._data)

    def to_dict(self):
        return dict(self._data)

    @classmethod
    def from_body(cls, body: bytes):
        return cls(**json.loads(body))


class RPCResponse(object):

    def __init__(self, trace_id, ok=True, result=None):
        self._data = {
            "trace_id": trace_id,
            "ok": ok,
            "result": result
        }

    @property
    def ok(self):
        return self._data["ok"]

    @property
    def trace_id(self):
        return self._data["trace_id"]

    @property
    def result(self):
        return self._data["result"]

    def to_json(self):
        return json.dumps(self._data)

    def to_dict(self):
        return dict(self._data)

    @classmethod
    def from_body(cls, body: bytes):
        return cls(**json.loads(body))


class ArupyRPCClient(object):

    def __init__(self, app: Arupy, queue_name):
        self.app = app
        self._caller_lock = threading.Lock()
        self._current_id = None
        self.queue_name = queue_name
        self.safe_publisher = None

        # response queue
        self._response_queue = queue.Queue()

    def _initial(self):
        self.safe_publisher: ArupySafePublisher = self.app.new_safe_publisher()
        result = self.safe_publisher.chan.queue_declare(queue="{}-callback".format(self.queue_name),
                                                        exclusive=True,
                                                        auto_delete=True)
        self.callback_queue = result.method.queue
        self.safe_publisher.chan.basic_consume(self._on_response, self.callback_queue, no_ack=True)

    def _call(self, target, vargs=None, kwargs=None, timeout=10, error_retry_times=3):
        tid = uuid.uuid4().hex
        self._current_id = tid
        request = RPCRequest(target, vargs or (), kwargs or {}, trace_id=tid)

        logger.info(request.to_json())

        for _ in range(error_retry_times):
            try:
                if not self.safe_publisher:
                    try:
                        self._initial()
                    except Exception:
                        logger.error(traceback.format_exc())
                        logger.warning("retrying in initial client.")
                        self.safe_publisher = None
                        continue

                self.safe_publisher.publish(
                    exchange="",
                    routing_key=self.queue_name,
                    properties=pika.BasicProperties(
                        reply_to=self.callback_queue,
                        correlation_id=tid
                    ),
                    body=request.to_json(),
                )

                self.safe_publisher.conn.process_data_events(timeout)
                break

            except Exception:
                logger.error(traceback.format_exc())
                logger.warning("retrying")
                self.safe_publisher = None
                continue

        while True:
            try:
                _ret = self._response_queue.get(timeout=0.2)
            except queue.Empty:
                return None

            if _ret:
                if _ret.trace_id == self._current_id:
                    return _ret


    def call(self, target, vargs=None, kwargs=None, timeout=10):
        with self._caller_lock:
            return self._call(target, vargs, kwargs, timeout)

    def _on_response(self, ch, method, props, body):
        response = None
        try:
            response = RPCResponse.from_body(body=body)
        except Exception:
            logger.error(traceback.format_exc())
        finally:
            self._response_queue.put(response)

class ArupyRPCConsumer(ArupyConsumer):

    def __init__(self, app: Arupy, queue_name, handler=None):
        ArupyConsumer.__init__(self, app=app)

        # sepecific queue_name
        self.queue_name = queue_name
        self.safe_publisher = self.app.new_safe_publisher()

        self.handler = handler

    def on_channel_created(self, channel: BlockingChannel):
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(queue=self.queue_name)

    def handle(self, channel, methods, props: pika.BasicProperties, body: bytes):
        try:
            request = RPCRequest.from_body(body)
        except Exception:
            channel.basic_ack(methods.delivery_tag)
            return

        try:

            target = getattr(self.handler, request.target)

            # no handler or target
            if (not self.handler) or (not target):
                self.safe_publisher.publish(
                    exchange="",
                    routing_key=props.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=props.correlation_id
                    ),
                    body=RPCResponse(trace_id=request.trace_id,
                                     ok=False,
                                     result="No Handler Backend.").to_json()
                )

            try:
                msg = target(*request.vargs, **request.kwargs)
            except Exception:
                msg = traceback.format_exc()

            raw = RPCResponse(trace_id=request.trace_id, ok=False, result=msg).to_json()

            self.safe_publisher.publish(
                exchange="",
                routing_key=props.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id
                ),
                body=raw,
            )
        except Exception:
            logger.error(traceback.format_exc())
        finally:
            channel.basic_ack(methods.delivery_tag)
