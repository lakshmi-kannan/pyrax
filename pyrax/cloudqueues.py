#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2013 Rackspace

# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
try:
    import simplejson as json
except ImportError:
    import json

from pyrax.client import BaseClient
from pyrax.manager import BaseManager
from pyrax.resource import BaseResource


#TODO: Use exc to define cloud queues exceptions
#TODO: Instead of using string ids, use utils.get_id to support both objects and strings
class CloudQueue(BaseResource):
    def __init__(self, manager, info, queue_name, key=None, loaded=False):
        super(CloudQueue, self).__init__(manager, info, key=key, loaded=loaded)
        self.queue_name = queue_name

    def set_queue_metadata(self, meta):
        """
        Replace the meta for the queue specified by queue_name
        with meta completely (PUT).
        """
        resp, body = self.manager.set_queue_metadata(self.queue_name)
        if resp.status == '204':
            self.meta = meta
        else:
            raise Exception('Unable to set meta for queue %s, HTTP status: %s' % (self.queue_name, resp.status))

    def get_queue_metadata(self):
        """
        Gets the metadata for the queue specified by queue_name.
        """
        if self.meta is not None:
            return self.meta
        else:
            resp, body = self.manager.get_queue_metadata(self.queue_name)
            if resp.status == '200':
                return body
            else:
                raise Exception('Unable to get metadata for queue %s, HTTP status: %s' % (self.queue_name, resp.status))

        return None

    def get_queue_stats(self):
        """
        Get statistics for queue specified by queue_name.
        """
        return self.manager.get_queue_stats(self.queue_name)

    def post_message(self, message):
        """
        Create a message in queue specified by queue_name.
        """
        resp, resp_body = self.manager.post_message(self.queue_name, message)
        if resp.status == '201':
            return resp_body
        else:
            raise Exception('Unable to POST message to queue %s', self.queue_name)

    def get_messages(self, marker=None, limit=20, echo=False, include_claimed=False):
        """
        Gets message(s) from the queue specified by queue_name.

        'limit' controls the number of messages returned.

        'marker' is used for paginated calls to specify the resumption point.

        'echo' is a boolean value (true or false) that determines whether the
        API returns a client's own messages, as determined by the uuid portion
        of the User-Agent header. If not specified, echo defaults to false.

        'include_claimed' is a boolean value (true or false) that determines
        whether the API returns claimed messages as well as unclaimed messages.
        If not specified, include_claimed defaults to faslse (only unclaimed
        messages are returned).
        """
        return self.manager.get_messages(self.queue_name, marker, limit, echo, include_claimed)

    def get_message_set(self, ids=None, claim_id=None):
        """
        Retrieves a set of messages from queue specified by queue_name.

        'ids' is an optional string that specifies the filter to be applied
        on messages returned.

        'claim_id' is an optional parameter that specified the claim ID to
        which the message is associated.
        """
        if ids is not None:
            if not isinstance(ids, basestring):
                try:
                    stringified_ids = ','.join(ids)
                    return self.manager.get_messages(self.queue_name, stringified_ids, claim_id)
                except TypeError:
                    raise Exception('ids should either be string or iterable')

        return self.manager.get_messages(self.queue_name, ids, claim_id)

    def delete_message_set(self, ids=None):
        """
        Deletes the messages in queue specified by queue_name

        'ids' is an optional parameter that can specify the messages ids
        to delete.
        """
        if ids is not None:
            if (isinstance(ids, basestring)):
                try:
                    stringified_ids = ','.join(ids)
                    return self.manager.delete_message_set(self.queue_name, stringified_ids)
                except TypeError:
                    raise Exception('ids should either be string or iterable')

        return self.manager.delete_message_set(self.queue_name, ids)

    def get_message(self, message_id, claim_id=None):
        """
        Gets a specific message specified by message_id from queue
        (specified by queue_name).
        """
        resp, body = self.manager.get_message(self.queue_name, message_id, claim_id)
        if resp.status == '200':
            return body
        else:
            raise Exception('Unable to get message from queue %s, message_id: %s'
                            % (self.queue_name, message_id))

    def delete_message(self, message_id, claim_id=None):
        """
        Deletes a message identified by message_id from queue
        specified by queue_name.
        """
        resp, body = self.manager.delete_queue(self.query_name, message_id, claim_id)
        if resp.status != '200':
            raise Exception('Failed deleting message with ID: %s from queue: %s' % (message_id, self.queue_name))

    def claim_messages(self, limit=10):
        """
        Claims a set of messages from queue specified by queue_name.

        'limit' controls the number of messages claimed in one call.
        """
        resp, body = self.manager.claim_messages(self.queue_name, limit)
        if resp.status == '201' or resp.status == '204':
            return body
        else:
            raise Exception('Unable to claim messages from queue :%s' % self.queue_name)

    def query_claim(self, claim_id):
        """
        Queries the specified claim for the specified queue.
        """
        resp, body = self.manager.query_claim(self.queue_name, claim_id)
        if resp.status == '200':
            return body
        else:
            raise Exception('Failed querying claim %s from queue %s' % (claim_id, self.queue_name))

    def update_claim(self, claim_id):
        """
        Updates the specified claim for the specified queue.
        """
        resp, body = self.manager.update_claim(self.queue_name, claim_id)
        if resp.status != '204':
            raise Exception('Failed updating claim %s for queue' % (claim_id, self.queue_name))

    def release_claim(self, claim_id):
        """
        Releases the specified claim for the specified queue.
        """
        resp, body = self.manager.release_claim(self.queue_name, claim_id)
        if resp.status != '200':
            raise Exception('Failed releasing claim %s for queue' % (claim_id, self.queue_name))


class CloudQueuesManager(BaseManager):
    def create_queue(self, queue_name):
        uri = '/%s' % (queue_name)
        resp, body = self.api.method_put(uri)

        status = resp['status']
        if status == '201':
            info = dict()
            info['id'] = queue_name
            return self.resource_class(self, info, queue_name)

        return None

    def set_queue_metadata(self, queue_name, meta):
        uri = '/%s/%s/metadata' % (self.uri_base, queue_name)
        body = meta
        resp, body = self.api.method_put(uri, body=body)
        return resp, body

    def get_queue_metadata(self, queue_name):
        uri = '/%s/%s/metadata' % (self.uri_base, queue_name)
        return self.api.method_get(uri)

    def get_queue_stats(self, queue_name):
        uri = '/%s/%s/stats' % (self.uri_base, queue_name)
        return self.api.method_get(uri)

    def post_message(self, queue_name, message):
        uri = '/%s/%s/messages' % (self.uri_base, queue_name)
        return self.api.method_post(uri, body=message)

    def get_messages(self, queue_name, marker=None, limit=None, echo=False, include_claimed=False):
        qparams = []
        if marker is not None:
            qparams.append('maker=' % marker)
        qparams.append('limit=' % int(limit))
        qparams.append('echo=' % bool(echo))
        qparams.append('include_claimed=' % bool(include_claimed))
        qparam = '&'.join(qparams)
        uri = '/%s/%s/messages?%s' % (self.uri_base, queue_name, qparam)

        return self.api.method_get(uri)

    def get_message_set(self, queue_name, ids=None, claim_id=None):
        qparams = []
        if ids is not None:
            qparams.append('ids=' % ids)
        if claim_id is not None:
            qparams.append('claim_id=' % claim_id)
        qparam = '&'.join(qparams)
        uri = '/%s/%s/messages?%s' % (self.uri_base, queue_name, qparam)
        return self.api.method_get(uri)

    def delete_message_set(self, queue_name, ids=None):
        qparams = []
        if ids is not None:
            qparams.append('ids=' % ids)
        qparam = '&'.join(qparams)
        uri = '/%s/%s/messages?%s' % (self.uri_base, queue_name, qparam)
        return self.api.method_delete(uri)

    def get_message(self, queue_name, message_id, claim_id=None):
        return self.get_message_set(queue_name, message_id, claim_id)

    def delete_message(self, queue_name, message_id, claim_id=None):
        return self.delete_message_set(queue_name, message_id, claim_id)

    def claim_messages(self, queue_name, limit=10, ttl=7200, grace=7200):
        qparams = []
        qparams.append('limit=' % int(limit))
        qparam = '&'.join(qparams)
        uri = '%s/%s/claims?%s' % (self.uri_base, queue_name, qparam)
        body = dict()
        body['ttl'] = ttl
        body['grace'] = grace
        return self.api.method_post(uri, body=json.dumps(body))

    def query_claim(self, queue_name, claim_id):
        uri = '/%s/%s/claims/%s' % (self.uri_base, queue_name, claim_id)
        return self.api.method_get(uri)

    def update_claim(self, queue_name, claim_id, claim_body, ttl=7200, grace=7200):
        body = dict()
        body['ttl'] = ttl
        body['grace'] = grace
        # TODO: This has to be PATCH
        uri = '/%s/%s/claims/%s' % (self.uri_base, queue_name, claim_id)
        return self.api.method_post(uri, body=json.dumps(body))

    def release_claim(self, queue_name, claim_id):
        uri = '/%s/%s/claims/%s' % (self.uri_base, queue_name, claim_id)
        return self.api.method_delete(uri)


class CloudQueuesClient(BaseClient):
    """
    This is the base client for CloudQueues
    """

    def __init__(self, *args, **kwargs):
        super(CloudQueuesClient, self).__init__(*args, **kwargs)
        self.name = "Cloud Queues"

    def _configure_manager(self):
        """
        Creates the Manager instance to handle networks.
        """
        self._cloud_queues_manager = CloudQueuesManager(
            self, uri_base="queues", resource_class=CloudQueue,
            response_key=None, plural_response_key=None)

    def create_queue(self, queue_name):
        # perform queue_name validation
        return self._cloud_queues_manager.create_queue(queue_name)

    def list_queues(self, marker=None, limit=20, detailed=False):
        qparams = []
        if marker is not None:
            qparams.append('maker=' % marker)
        qparams.append('limit=' % int(limit))
        qparams.append('detailed=' % bool(detailed))
        qparam = '&'.join(qparams)
        uri = '?%s' % (qparam)
        resp, body = self.api.method_get(uri)
        return resp, body

    def delete_queue(self, queue_name):
        uri = '/%s' % (queue_name)
        resp, body = self.api.method_delete(uri)
        return resp, body

    def check_queue_exists(self, queue_name):
        uri = '/%s' % (queue_name)
        resp, body = self.api.method_get(uri)
        return resp, body

    def set_queue_metadata(self, queue_name, meta):
        return self._cloud_queues_manager.set_queue_metadata(queue_name, meta)

    def get_queue_metadata(self, queue_name):
        return self._cloud_queues_manager.get_queue_metadata(queue_name)

    def get_queue_stats(self, queue_name):
        return self._cloud_queues_manager.get_queue_stats(queue_name)

    def post_message(self, queue_name, message):
        return self._cloud_queues_manager.post_message(queue_name, message)

    def get_messages(self, queue_name, marker=None, limit=20, echo=False, include_claimed=False):
        return self._cloud_queues_manager.get_messages(queue_name, marker, limit, echo, include_claimed)

    def get_message_set(self, queue_name, ids=None, claim_id=None):
        # TODO: Stringify list
        return self._cloud_queues_manager.get_message_set(queue_name, ids, claim_id)

    def delete_message_set(self, queue_name, ids=None):
        return self._cloud_queues_manager.delete_message_set(queue_name, ids)

    def get_message(self, queue_name, message_id, claim_id=None):
        return self._cloud_queues_manager.get_message(queue_name, message_id, claim_id)

    def delete_message(self, queue_name, message_id, claim_id=None):
        return self._cloud_queues_manager.delete_message(queue_name, message_id, claim_id)

    def claim_messages(self, queue_name, limit=10):
        return self._cloud_queues_manager.claim_messages(queue_name, limit)

    def query_claim(self, queue_name, claim_id):
        return self._cloud_queues_manager.query_claim(queue_name, claim_id)

    def update_claim(self, queue_name, claim_id):
        return self._cloud_queues_manager.update_claim(queue_name, claim_id)

    def release_claim(self, queue_name, claim_id):
        return self._cloud_queues_manager.release_claim(queue_name, claim_id)
