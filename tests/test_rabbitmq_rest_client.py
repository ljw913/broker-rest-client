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

from broker_rest_client.models import RabbitMQUser, RabbitMQUserPermissions
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


def test_vhost():
    client = RabbitMQRestClient(request_handler=Mock())
    assert "%2F" == client.vhost


def test_create_topic():
    durable, auto_delete = False, False
    topic_name = 'some topic'

    data = {
            "type": "topic",
            "durable": durable,
            "auto_delete": auto_delete,
            "internal": False,
            "arguments": {}
        }

    client = RabbitMQRestClient(request_handler=Mock())
    mock_request = Mock()
    client.perform_request = mock_request

    client.create_topic(topic_name)

    mock_request.assert_called_once_with('PUT', f'api/exchanges/{client.vhost}/{topic_name}', json=data)


def test_delete_topic():
    client = RabbitMQRestClient(request_handler=Mock())
    mock_request = Mock()
    client.perform_request = mock_request

    topic_name = 'some topic'

    client.delete_topic(topic_name)

    mock_request.assert_called_once_with('DELETE', f'api/exchanges/{client.vhost}/{topic_name}')


def test_get_queue():
    client = RabbitMQRestClient(request_handler=Mock())
    mock_request = Mock()
    client.perform_request = mock_request

    queue_name = 'some queue'

    client.get_queue(queue_name)

    mock_request.assert_called_once_with('GET', f'api/queues/{client.vhost}/{queue_name}')


def test_create_queue_with_max_length():
    durable, auto_delete = False, False
    queue_name = 'some topic'
    max_length = 10

    data = {
        "durable": durable,
        "auto_delete": auto_delete,
        "arguments": {
            'x-max-length': max_length
        }
    }

    client = RabbitMQRestClient(request_handler=Mock())
    mock_request = Mock()
    client.perform_request = mock_request

    client.create_queue(queue_name, max_length)

    mock_request.assert_called_once_with('PUT', f'api/queues/{client.vhost}/{queue_name}', json=data)


def test_create_queue_without_max_length():
    durable, auto_delete = False, False
    queue_name = 'some topic'

    data = {
        "durable": durable,
        "auto_delete": auto_delete,
        "arguments": {}
    }

    client = RabbitMQRestClient(request_handler=Mock())
    mock_request = Mock()
    client.perform_request = mock_request

    client.create_queue(queue_name)

    mock_request.assert_called_once_with('PUT', f'api/queues/{client.vhost}/{queue_name}', json=data)


def test_delete_queue():
    client = RabbitMQRestClient(request_handler=Mock())
    mock_request = Mock()
    client.perform_request = mock_request

    queue_name = 'some queue'

    client.delete_queue(queue_name)

    mock_request.assert_called_once_with('DELETE', f'api/queues/{client.vhost}/{queue_name}')


@pytest.mark.parametrize('topic_name, expected_topic_name', [
    ('default', 'amq.topic'),
    ('any_other_topic_name', 'any_other_topic_name')
])
def test_bind_queue_to_topic(topic_name, expected_topic_name):
    queue_name = 'some queue'
    durable = False
    key = 'routing key'

    data = {
        "routing_key": key,
        "arguments": {
            "durable": durable
        }
    }

    client = RabbitMQRestClient(request_handler=Mock())
    mock_request = Mock()
    client.perform_request = mock_request

    client.bind_queue_to_topic(queue_name, key, topic_name, durable)

    mock_request.assert_called_once_with('POST',
                                         f'api/bindings/{client.vhost}/e/{expected_topic_name}/q/{queue_name}',
                                         json=data)


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


@pytest.mark.parametrize('topic_name, expected_topic_name', [
    ('default', 'amq.topic'),
    ('any_other_topic_name', 'any_other_topic_name')
])
def test_delete_binding__url_contains_properties_key_of_binding(topic_name, expected_topic_name):
    properties_key = 'props'
    queue_name = 'queue'
    key = 'routing_key'

    client = RabbitMQRestClient(request_handler=Mock())

    client.get_queue_bindings = Mock(
        return_value=[{'source': 'topic1', 'routing_key': 'key1', 'properties_key': properties_key}]
    )

    mock_request = Mock()
    client.perform_request = mock_request

    client.delete_queue_binding(queue_name, topic_name, key)

    mock_request.assert_called_once_with('DELETE',
                                         f'api/bindings/%2F/e/{expected_topic_name}/q/{queue_name}/{properties_key}')


@pytest.mark.parametrize('user_dict, expected_user', [
    (
        {
            "name": "rabbitmq",
            "tags": "administrator,management"
        },
        RabbitMQUser(name="rabbitmq", tags=["administrator", "management"])
    )
])
def test_get_user__user_exists_and_is_returned(user_dict, expected_user):

    response = Mock()
    response.status_code = 200
    response.content = user_dict
    response.json = Mock(return_value=user_dict)

    request_handler = Mock()
    request_handler.get = Mock(return_value=response)

    client = RabbitMQRestClient(request_handler=request_handler)

    user = client.get_user('rabbitmq')

    assert expected_user == user

    called_url = request_handler.get.call_args[0][0]
    assert 'api/users/rabbitmq' == called_url


@pytest.mark.parametrize('error_code', [400, 401, 403, 404, 500])
def test_get_user__http_error_code__raises_api_error(error_code):
    response = Mock()
    response.status_code = error_code

    request_handler = Mock()
    request_handler.get = Mock(return_value=response)

    client = RabbitMQRestClient(request_handler=request_handler)

    with pytest.raises(APIError):
        client.get_user('name')


@pytest.mark.parametrize('get_user_response, exists', [
    (RabbitMQUser('name'), True),
    (APIError('error', 400), False),
    (APIError('error', 401), False),
    (APIError('error', 403), False),
    (APIError('error', 404), False),
    (APIError('error', 500), False)
])
def test_user_exists(get_user_response, exists):
    request_handler = Mock()

    client = RabbitMQRestClient(request_handler=request_handler)
    if isinstance(get_user_response, APIError):
        client.get_user = Mock(side_effect=get_user_response)
    else:
        client.get_user = Mock(return_value=get_user_response)
    assert exists == client.user_exists('name')


@pytest.mark.parametrize('error_code', [400, 401, 403, 404, 500])
def test_add_user__http_error_code__raises_api_error(error_code):
    response = Mock()
    response.status_code = error_code

    request_handler = Mock()
    request_handler.put = Mock(return_value=response)

    client = RabbitMQRestClient(request_handler=request_handler)

    with pytest.raises(APIError):
        client.add_user('name', 'password', RabbitMQUserPermissions(configure=".*", write=".*", read=".*"))


def test_add_user():
    mock_create_user = Mock()
    mock_set_user_permissions = Mock()

    client = RabbitMQRestClient(request_handler=Mock())
    client.create_user = mock_create_user
    client.set_user_permissions = mock_set_user_permissions

    name = 'username'
    password = 'password'
    tags = ['administrator']
    permissions = RabbitMQUserPermissions(configure=".*", write=".*", read=".*")

    client.add_user(name, password, permissions, tags)

    mock_create_user.assert_called_once_with(name, password, tags)
    mock_set_user_permissions.assert_called_once_with(name, permissions)


@pytest.mark.parametrize('error_code', [400, 401, 403, 404, 500])
def test_create_user__http_error_code__raises_api_error(error_code):
    response = Mock()
    response.status_code = error_code

    request_handler = Mock()
    request_handler.put = Mock(return_value=response)

    client = RabbitMQRestClient(request_handler=request_handler)

    with pytest.raises(APIError):
        client.create_user('name', 'password')


@pytest.mark.parametrize('name, password, tags, expected_data', [
    ('username', 'password', None, {'password': 'password', 'tags': ""}),
    ('username', 'password', ['administrator', 'management'],
     {'password': 'password', 'tags': "administrator management"}),
])
def test_create_user(name, password, tags, expected_data):
    client = RabbitMQRestClient(request_handler=Mock())

    mock_request = Mock()
    client.perform_request = mock_request

    client.create_user(name, password, tags)

    mock_request.assert_called_once_with('PUT', f'api/users/{name}', json=expected_data)


@pytest.mark.parametrize('name, permissions, expected_data', [
    (
        'username',
        RabbitMQUserPermissions(configure=".*", write=".*", read=".*"),
        {'configure': '.*', 'write': ".*", 'read': ".*"}
    ),
])
def test_set_user_permissions(name, permissions, expected_data):
    client = RabbitMQRestClient(request_handler=Mock())

    mock_request = Mock()
    client.perform_request = mock_request

    client.set_user_permissions(name, permissions)

    mock_request.assert_called_once_with('PUT', f'api/permissions/{client.vhost}/{name}', json=expected_data)
