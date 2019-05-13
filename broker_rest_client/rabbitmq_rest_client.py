"""
Copyright 2019 EUROCONTROL
==========================================

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the 
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following 
   disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following 
   disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products 
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

==========================================

Editorial note: this license is an instance of the BSD license template as provided by the Open Source Initiative: 
http://opensource.org/licenses/BSD-3-Clause

Details on EUROCONTROL: http://www.eurocontrol.int
"""
import typing as t
from urllib.parse import quote

from requests import Response
from rest_client import Requestor, ClientFactory
from rest_client.typing import RequestHandler

__author__ = "EUROCONTROL (SWIM)"


class RabbitMQRestClient(Requestor, ClientFactory):

    def __init__(self, request_handler: RequestHandler) -> None:
        Requestor.__init__(self, request_handler)
        self._request_handler = request_handler

        self._vhost = "/"

    @property
    def vhost(self):
        return quote(self._vhost, safe='')

    @vhost.setter
    def vhost(self, value):
        self._vhost = value

    def _get_create_topic_url(self, name):
        return f'api/exchanges/{self.vhost}/{name}'

    def _get_create_queue_url(self, name):
        return f'api/queues/{self.vhost}/{name}'

    def _get_bind_queue_url(self, queue, topic):
        return f'api/bindings/{self.vhost}/e/{topic}/q/{queue}'

    def _get_delete_queue_url(self, name):
        return f'api/queues/{self.vhost}/{name}'

    def _get_bindings_url(self):
        return f'api/bindings'

    def create_topic(self, name: str, durable: t.Optional[bool] = False) -> Response:
        url = self._get_create_topic_url(name)
        data = {
            "type": "topic",
            "durable": durable,
            "auto_delete": False,
            "internal": False,
            "arguments": {}
        }

        return self.perform_request('PUT', url, json=data)

    def create_queue(self, name: str, durable: t.Optional[bool] = False) -> Response:
        url = self._get_create_queue_url(name)
        data = {
            "durable": durable,
            "auto_delete": False,
            "arguments": {},
            "node": "rabbit@my-rabbit"
        }

        return self.perform_request('PUT', url, json=data)

    def bind_queue(self, queue: str, topic: str, key: str, durable: t.Optional[bool] = False) -> Response:
        if topic == 'default':
            topic = 'amq.topic'

        url = self._get_bind_queue_url(queue, topic)
        data = {
            "routing_key": key,
            "arguments": {
                "durable": durable
            }
        }

        return self.perform_request('POST', url, json=data)

    def delete_queue(self, name: str) -> Response:
        url = self._get_delete_queue_url(name)

        return self.perform_request('DELETE', url)

    def get_bindings(self):
        url = self._get_bindings_url()

        response = self.perform_request('GET', url)

        return response