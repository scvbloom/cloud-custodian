# Copyright The Cloud Custodian Authors.
# SPDX-License-Identifier: Apache-2.0
"""
Actions to take on resources
"""
import copy
import logging
import sys
from abc import ABC, abstractmethod
from concurrent.futures import as_completed

from c7n.element import Element
from c7n.exceptions import PolicyValidationError, ClientError
from c7n.registry import PluginRegistry
from c7n.utils import format_string_values


class ActionRegistry(PluginRegistry):

    def __init__(self, *args, **kw):
        super(ActionRegistry, self).__init__(*args, **kw)
        # Defer to provider initialization of registry
        from .webhook import Webhook
        self.register('webhook', Webhook)

    def parse(self, data, manager):
        results = []
        for d in data:
            results.append(self.factory(d, manager))
        return results

    def factory(self, data, manager):
        if isinstance(data, dict):
            action_type = data.get('type')
            if action_type is None:
                raise PolicyValidationError(
                    "Invalid action type found in %s" % (data))
        else:
            action_type = data
            data = {}

        action_class = self.get(action_type)
        if action_class is None:
            raise PolicyValidationError(
                "Invalid action type %s, valid actions %s" % (
                    action_type, list(self.keys())))
        # Construct a ResourceManager
        return action_class(data, manager)


class Action(Element):

    log = logging.getLogger("custodian.actions")

    def __init__(self, data=None, manager=None, log_dir=None):
        self.data = data or {}
        self.manager = manager
        self.log_dir = log_dir

    @property
    def name(self):
        return self.__class__.__name__.lower()

    def process(self, resources):
        raise NotImplementedError(
            "Base action class does not implement behavior")

    def _run_api(self, cmd, *args, **kw):
        try:
            return cmd(*args, **kw)
        except ClientError as e:
            if (e.response['Error']['Code'] == 'DryRunOperation' and
            e.response['ResponseMetadata']['HTTPStatusCode'] == 412 and
            'would have succeeded' in e.response['Error']['Message']):
                return self.log.info(
                    "Dry run operation %s succeeded" % (
                        self.__class__.__name__.lower()))
            raise


BaseAction = Action


class EventAction(BaseAction):
    """Actions which receive lambda event if present
    """
class BaseVariableSupportAction(ABC, BaseAction):
    """Actions which have native support for variable interpolation
    """

    def process(self, resources):
        tmpl = super().__getattribute__("data")  # to prevent false-positive warnings from __getattribute__ below
        with self.executor_factory(max_workers=3) as w:
            futures = {}
            results = []
            for resource in resources:
                additional_args = self.get_additional_format_arguments(resource) or {}
                vars = self._merge(copy.deepcopy(resource), additional_args)
                data = format_string_values(tmpl, **vars)
                futures[w.submit(self.process_resource, data, resource)] = resource

            for f in as_completed(futures):
                if f.exception():
                    self.log.error('error occurred while processing resource:\n%s', f.exception())
                results += filter(None, [f.result()])

            return results

    @staticmethod
    def _merge(a, b):
        if sys.version_info >= (3, 5):
            return {**a, **b}
        else:
            c = a.copy()
            c.update(b)
            return c

    def get_additional_format_arguments(self, resource):
        """Return additional arguments from this resource
        that are then used to process data template later passed to process_resource.
        This is an optional hook that subclasses can tap into to override certain keys.
        """
        pass

    @abstractmethod
    def process_resource(self, data, resource):
        """Process an individual record with pre-processed data value
        returning a result from the action. The data value passed is pre-formatted from value
        in self.data and subclasses MUST use this instead of self.data .
        This method can be called concurrently and it's implementations must be thread-safe.
        """
        pass

    def __getattribute__(self, attr):
        if attr == "data":
            # a latch to allow us to detect what all downstream actions are depending on self.data directly
            # going forward we might want to enforce this and replace with an AttributeError or similar
            self.log.warning("direct access to attribute 'data' detected from action %s", self.name)
        return super().__getattribute__(attr)