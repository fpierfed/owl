#!/usr/bin/env python
import logging
import socket
import sys

import pika

from owl import blackboard




# Constants
HOSTNAME = socket.gethostname()
logging.basicConfig(level=logging.CRITICAL)
CLUSTER_ID = blackboard.getMaxClusterId()




def get_clusterid():
    global CLUSTER_ID
    CLUSTER_ID += 1
    return(CLUSTER_ID)


def on_request(ch, method, props, body):
    if(body == 'get_clusterid'):
        clusterid = get_clusterid()
        print(' [.] got key request, replying with key=%d' % (clusterid))
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                         body=str(clusterid))
        ch.basic_ack(delivery_tag = method.delivery_tag)



if(__name__ == '__main__'):
    try:
        broker_host = sys.argv[1]
    except:
        print(' [i] No broker hostname specified, using localhost.')
        print(' [i] You can specify a hostname for the message broker as ' + \
              'first argument to this')
        print(' [i] script e.g. ./spread_keymaster.py machine.example.com')
        broker_host = 'localhost'

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=broker_host))
    channel = connection.channel()
    channel.queue_declare(queue='key_queue')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_request, queue='key_queue')

    print " [x] Awaiting KEY requests"
    channel.start_consuming()
