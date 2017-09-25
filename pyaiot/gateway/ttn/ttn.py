# Copyright 2017 IoT-Lab Team
# Contributor(s) : see AUTHORS file
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""TTN MQTT backend management module."""

import time
import uuid
import datetime
import json
import asyncio
import logging
import base64

from tornado import gen
from tornado.options import options

from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_1

from pyaiot.common.messaging import Message as Msg

logger = logging.getLogger("pyaiot.gw.ttn")


MQTT_HOST = 'eu.thethings.network'
MQTT_PORT = 1883
PROTOCOL = "LoRa TTN"

MQTT_CLIENT_CONFIG = {
    'keep_alive': 20,
    'ping_delay': 10,
    'auto_reconnect': True,
    'reconnect_max_interval': 20,
    'reconnect_retries': 10,
}


class TTNNode(object):
    """Object defining a TTN node."""

    def __init__(self, identifier, check_time=time.time(), resources=[]):
        self.node_id = identifier
        self.check_time = check_time
        self.resources = resources

    def __eq__(self, other):
        return self.node_id == other.node_id

    def __neq__(self, other):
        return self.node_id != other.node_id

    def __hash__(self):
        return hash(self.node_id)

    def __repr__(self):
        return("Node '{}', Last check: {}, Resources: {}"
               .format(self.node_id, self.check_time, self.resources))


class TTNController():
    """TTN controller with MQTT client inside."""

    def __init__(self, on_message_cb):
        # on_message_cb = send_to_broker method in gateway application
        self._on_message_cb = on_message_cb
        self.nodes = {}
        self.mqtt_client = MQTTClient(config=MQTT_CLIENT_CONFIG)
        asyncio.get_event_loop().create_task(self.start())

    @asyncio.coroutine
    def start(self):
        """Connect to MQTT broker and subscribe to node check ressource."""
        mqtt_url = 'mqtt://{}:{}@{}:{}'.format(options.app_id, options.app_key,
                                               MQTT_HOST, MQTT_PORT)

        yield from self.mqtt_client.connect(mqtt_url)
        yield from self.mqtt_client.subscribe([('+/devices/+/up', QOS_1)])
        while True:
            try:
                logger.debug("Waiting for MQTT messages published by TTN "
                             "MQTT backend")
                # Blocked here until a message is received
                message = yield from self.mqtt_client.deliver_message()
            except ClientException as ce:
                logger.error("Client exception: {}".format(ce))
                break
            except Exception as exc:
                logger.error("General exception: {}".format(exc))
                break
            packet = message.publish_packet

            try:
                data = json.loads(packet.payload.data.decode())
            except:
                # Skip data if not valid
                continue
            logger.debug("Received message from TTN backend => {}"
                         .format(data))
            self.handle_node_update(data)

    def close(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._disconnect())

    @asyncio.coroutine
    def _disconnect(self):
        for node in self.nodes:
            yield from self._disconnect_from_node(node)
        yield from self.mqtt_client.disconnect()

    @gen.coroutine
    def fetch_nodes_cache(self, source):
        """Send cached nodes information."""
        logger.debug("Fetching cached information of registered nodes '{}'."
                     .format(self.nodes))
        for _, value in self.nodes.items():
            self._on_message_cb(Msg.new_node(value['uid'], dst=source))
            for resource, data in value['data'].items():
                self._on_message_cb(
                    Msg.update_node(value['uid'], resource, data,
                                    dst=source))

    def handle_node_update(self, data):
        """Handle CoAP post message sent from coap node."""
        payload = base64.b64decode(data['payload_raw']).decode()
        node_id = data['dev_id']
        app_id = data['app_id']
        last_update = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.debug("node id: {}\npayload: {}\nlast update: {}"
                     .format(node_id, payload, last_update))

        node = TTNNode(node_id)
        if node not in self.nodes:
            node_uid = str(uuid.uuid4())
            self.nodes.update({
                node: {
                    'uid': node_uid,
                    'data': {
                        'protocol': PROTOCOL,
                        'dev_id': node_id,
                        'app_id': app_id,
                        'last_update': last_update
                    }
                }
            })
            logger.debug("Available nodes: {}".format(self.nodes))
            self._on_message_cb(Msg.new_node(node_uid))
            self._on_message_cb(Msg.update_node(node_uid, "protocol",
                                                PROTOCOL))
            self._on_message_cb(Msg.update_node(node_uid, "dev_id",
                self.nodes[node]['data']['dev_id']))
            self._on_message_cb(Msg.update_node(node_uid, "app_id",
                self.nodes[node]['data']['app_id']))
            self._on_message_cb(Msg.update_node(node_uid, "last_update",
                self.nodes[node]['data']['last_update']))

        # Send update to broker
        self._on_message_cb(Msg.update_node(
            self.nodes[node]['uid'], 'dev_id', node_id))

        self._on_message_cb(Msg.update_node(
            self.nodes[node]['uid'], 'app_id', app_id))

        self._on_message_cb(Msg.update_node(
            self.nodes[node]['uid'], 'last_update', last_update))
        
        try:
            resource_data = json.loads(payload)
            resource = resource_data['r']
            value = resource_data['v']
            if resource in self.nodes[node]['data']:
                # Add updated information to cache
                self.nodes[node]['data'][resource] = value
            else:
                self.nodes[node]['data'].update({resource: value})
            self._on_message_cb(Msg.update_node(
                self.nodes[node]['uid'], resource, value))
        except:
            logger.debug("Invalid resource payload: {}".format(payload))

    @asyncio.coroutine
    def _disconnect_from_node(self, node):
        yield from self.mqtt_client.unsubscribe(
            ['node/{}/resource'.format(node.node_id)])
        for resource in node.resources:
            yield from self.mqtt_client.unsubscribe(
                ['node/{}/{}'.format(node.node_id, resource)])
