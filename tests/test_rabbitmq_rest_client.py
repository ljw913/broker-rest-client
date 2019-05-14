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
from unittest.mock import Mock

import pytest
from rest_client.errors import APIError

from broker_rest_client.rabbitmq_rest_client import RabbitMQRestClient

__author__ = "EUROCONTROL (SWIM)"


@pytest.fixture()
def bindings():
    return [
        {'source': 'topic1', 'routing_key': 'key1'},
        {'source': 'topic1', 'routing_key': 'key2'},
        {'source': 'topic2', 'routing_key': 'key1'},
        {'source': 'topic2', 'routing_key': 'key2'},
        {'source': 'topic3', 'routing_key': 'key1'},
        {'source': 'topic3', 'routing_key': 'key2'},
        {'source': 'topic4', 'routing_key': 'key1'},
        {'source': 'topic4', 'routing_key': 'key2'},
        {'source': 'topic5', 'routing_key': 'key1'},
        {'source': 'topic5', 'routing_key': 'key2'},
    ]

@pytest.mark.parametrize('topic, key, expected_binding', [
    ('topic1', 'key1', {'source': 'topic1', 'routing_key': 'key1'}),
    ('topic2', 'key2', {'source': 'topic2', 'routing_key': 'key2'}),
    ('topic3', 'key1', {'source': 'topic3', 'routing_key': 'key1'}),
    ('topic4', 'key1', {'source': 'topic4', 'routing_key': 'key1'}),
    ('topic5', 'key1', {'source': 'topic5', 'routing_key': 'key1'})
])
def test_get_queue_bindings__filter_is_applied_on_topic_and_key(bindings, topic, key, expected_binding):

    response = Mock()
    response.status_code = 200
    response.content = bindings
    response.json = Mock(return_value=bindings)

    request_handler = Mock()
    request_handler.get = Mock(return_value=response)

    client = RabbitMQRestClient(request_handler=request_handler)

    assert [expected_binding] == client.get_queue_bindings('queue', topic, key)


def test_delete_binding__no_binding_is_found__raises_404():
    client = RabbitMQRestClient(request_handler=Mock())

    client.get_queue_bindings = Mock(return_value=[])

    with pytest.raises(APIError) as e:
        client.delete_queue_binding('queue', 'topic', 'key')
    assert "[404] - No binding found between topic 'topic' and queue 'queue' with name 'key'" == str(e.value)


def test_delete_binding__url_contains_properties_key_of_binding():
    properties_key = 'props'

    client = RabbitMQRestClient(request_handler=Mock())

    client.get_queue_bindings = Mock(
        return_value=[{'source': 'topic1', 'routing_key': 'key1', 'properties_key': properties_key}]
    )

    mock_request = Mock()
    client.perform_request = mock_request

    client.delete_queue_binding('queue', 'topic', 'key')

    mock_request.assert_called_once_with('DELETE', f'api/bindings/%2F/e/topic/q/queue/{properties_key}')
