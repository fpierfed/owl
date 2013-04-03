#!/usr/bin/env python
import uuid

import pika

from spread.client import RpcClient


# Constants
QUEUE_NAME = 'key_queue'




def get_clusterid(client=None, host='localhost'):
    if(client is None):
        client = RpcClient(host=host, fast=True)

    client.response = None
    client._rpccall_id = str(uuid.uuid4())
    client.channel.basic_publish(exchange='',
                               routing_key=QUEUE_NAME,
                               properties=pika.BasicProperties(
                                     reply_to = client.callback_queue,
                                     correlation_id = client._rpccall_id),
                               body='get_clusterid')
    while(not client.is_ready()):
        client.wait()
    return(client.result())



