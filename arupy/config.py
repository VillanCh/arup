#!/usr/bin/env python3
# coding:utf-8
import pika
import os
import re
import argparse
import yaml
import traceback


def _init_yaml_loader_with_env():
    """
    Load yaml with env:
      $ENV_VAR:DEFAULT_VALUE or ${ENV_VAR:DEFAULT_VALUE}
    """
    pattern = re.compile('^\$\{(.*?)(:(.*))?\}$')
    yaml.add_implicit_resolver("!env", pattern)

    def env_construction(loader, node):
        value = loader.construct_scalar(node)
        env_var_name, _, default = pattern.match(value).groups()
        env_var = os.getenv(env_var_name, default)
        if not env_var:
            raise ValueError('the environment variable: {} is not existed.'.format(env_var_name))
        return env_var
    yaml.add_constructor('!env', env_construction)

    pattern2 = re.compile('^\$(.*?)(:(.*))?$')
    yaml.add_implicit_resolver('!env2', pattern2)

    def env2_construction(loader, node):
        value = loader.construct_scalar(node)
        env_var_name, _, default = pattern2.match(value).groups()
        env_var = os.getenv(env_var_name, default)
        if not env_var:
            raise ValueError('the environment variable: {} is not existed.'.format(env_var_name))
        return env_var
    yaml.add_constructor('!env2', env2_construction)


_init_yaml_loader_with_env()

mq_config = """
rabbit_connection:
  host: ${RABBITMQ_HOST:127.0.0.1}
  port: ${RABBITMQ_PORT:5672}
  virtual_host: ${RABBITMQ_VIRTUAL_HOST:/}
  ssl: false
  ssl_options:
    certfile: client/cert.pem
    keyfile: client/key.pem
    ca_certs: Null
rabbit_credentials:
  username: ${RABBITMQ_USER:guest}
  password: ${RABBITMQ_PASS:guest}
"""


def parse_mq_parameters_from_options(options):
    if os.path.exists(options.mq_conf):
        return parse_mq_parameters_from_file(options)
    else:
        username = options.mq_user
        password = options.mq_pass
        creds = pika.PlainCredentials(username, password)
        return pika.ConnectionParameters(credentials=creds, host=options.mq_host,
                                         port=int(options.mq_port), virtual_host=options.mq_vhost)


def parse_mq_parameters_from_file(yaml_config):
    with open(yaml_config) as fp:
        obj = yaml.load(fp.read())
    creds = pika.PlainCredentials(**obj["rabbit_credentials"])
    obj["rabbit_connection"]['port'] = int(obj["rabbit_connection"].get('port', 5672))
    return pika.ConnectionParameters(credentials=creds, **obj["rabbit_connection"])


def cli():
    parser = argparse.ArgumentParser()

    # mq options
    parser.add_argument('--mq-user', type=str, default='pydra-user', dest='mq_user')
    parser.add_argument('--mq-pass', type=str, default='pydra-pass', dest='mq_pass')
    parser.add_argument('--mq-host', type=str, default='127.0.0.1', dest='mq_host')
    parser.add_argument('--mq-vhost', type=str, default='pydra', dest='mq_vhost')
    parser.add_argument('--mq-port', type=int, default=5672, dest='mq_port')

    # mq config
    parser.add_argument("--mq-config", type=str, default="config.yml", dest="mq_conf")

    options = parser.parse_args()

    try:
        mq_conn_param = parse_mq_parameters_from_options(options)
        print(mq_conn_param)
    except Exception:
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    cli()
