from .cli import run
import json
import os
from pyArango.connection import Connection


class CloudProvider():
    fetch_query = '''
    FOR cloud_policies IN policies
        FOR policy IN cloud_policies.policies
            FILTER policy.resource == @resource
                RETURN policy
    '''

    fetch_periodic = '''
    FOR cloud_policies IN policies
        FOR policy IN cloud_policies.policies
            FILTER LIKE(policy.resource, @resource)
            FILTER HAS(policy, "mode")
            FILTER policy.mode.type == "periodic"
                RETURN policy
    '''
    event_id = ""

    def _connect(self):
        self.conn = Connection(
            arangoURL=os.getenv("CUSTODIAN_ARANGO_URL"),
            username=os.getenv("CUSTODIAN_ARANGO_USERNAME"),
            password=os.getenv("CUSTODIAN_ARANGO_PASSWORD"))
        self.db = self.conn[os.getenv("CUSTODIAN_ARANGO_DBNAME")]

    def __init__(
            self, config, output_dir, accounts=None,
            tags=None, region=None, policy="", policy_tags=(),
            cache_period=15, cache_path=None, metrics=False,
            dryrun=False, debug=False, verbose=False,
            metrics_uri=None):

        self._connect()

        self.config = self._parse_config(config)
        self.use = self._parse_use()
        self.output_dir = output_dir
        self.accounts = accounts
        self.tags = tags
        self.region = region
        self.policy = policy
        self.policy_tags = policy_tags
        self.cache_period = cache_period
        self.cache_path = cache_path
        self.metrics = metrics,
        self.dryrun = dryrun
        self.debug = debug
        self.verbose = verbose
        self.metrics_uri = metrics_uri

    def _filter_periodic_policies(self, cloud):
        if cloud == "":
            raise EOFError
        # LIKE() search
        resource = cloud + "%"
        print(resource)
        return self.db.AQLQuery(
            self.fetch_periodic,
            bindVars={"resource": resource}, rawResults=True)

    def _filter_policies(self, resource):
        return self.db.AQLQuery(
            self.fetch_query,
            bindVars={"resource": resource}, rawResults=True)

    def _parse_config(self, config):
        raise NotImplementedError

    def _parse_use(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError


class AWS(CloudProvider):
    def _parse_config(self, config):
        account_config = json.loads(config)
        c = {
            "event_id": account_config["id"],
            "source": account_config["source"],
            "account_id": account_config["account"],
            "regions": [account_config["region"]],
            "name": "aws_" + account_config["account"],
        }
        aws_config = {
            "accounts": [c]
        }
        return aws_config

    def _parse_use(self):
        source_policies = []
        periodic = self.config["accounts"][0]["source"] == "periodic"
        if periodic:
            source_policies = self._filter_periodic_policies("aws")
            print(source_policies)
            return {
                "policies": source_policies,
            }

        source_policies = self._filter_policies(self.config["accounts"][0]["source"])

        print(source_policies)
        return {
            "policies": source_policies,
        }

    def run(self):
        return run(self.config, self.use, self.output_dir,
        self.accounts, self.tags, self.region, self.policy,
        self.policy_tags, self.cache_period, self.cache_path,
        self.metrics, self.dryrun, self.debug, self.verbose,
        self.metrics_uri), self.config["accounts"][0]


def overrun(
        config, output_dir, accounts=None, tags=None,
        region=None, policy="", policy_tags=None,
        cache_period=15, cache_path=None, metrics=False,
        dryrun=False, debug=False, verbose=False,
        metrics_uri=None, cloud="aws"):
    cld = None
    if cloud == "aws":
        cld = AWS(
            config, output_dir, accounts, tags, region,
            policy, policy_tags, cache_period, cache_path,
            metrics, dryrun, debug, verbose, metrics_uri)

    return cld.run()
