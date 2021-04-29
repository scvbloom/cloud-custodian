import json
from c7n.filters.core import Filter
from c7n.resources.iam import Role
from c7n.resources.vpc import VpcEndpoint
from c7n.utils import type_schema, local_session
from arnparse import arnparse


@Role.filter_registry.register("has-allow-all")
class HasAllowAllForAnyService(Filter):
    """Check IAM permissions associated with the role and see if any permission contains {action: "<service>:*"}

    The policy filters IAM role objects on the basis of IAM Policies attached to that role,
    if the policy contains an {action: "<service>:*"} block

    .. code-block:: yaml

    - name: iam-user-with-password-enabled
      resource: aws.iam-role
      filters:
      - type: has-allow-all
    """

    schema = type_schema('has-allow-all')
    permissions = ('iam:ListPolicies', 'iam:ListPolicyVersions')

    def _policy_has_allow_all(self, policy):
        statements = policy.document['Statement']
        if isinstance(statements, dict):
            statements = [statements]

        for stmt in statements:
            actions = stmt['Action']
            if isinstance(actions, str):
                actions = [actions]

            for action in actions:
                if ':' in action:
                    service, api = action.split(sep=':')
                    if service == "*" or api == "*":
                        return True
                elif action == "*":
                    return True
        return False

    def _any_policy_has_allow_all(self, policies):
        for p in policies:
            has = self._policy_has_allow_all(p.default_version) if p.default_version else False
            if has:
                return True
        return False

    def process(self, resources, event=None):
        iam = local_session(self.manager.session_factory).resource('iam')
        return [
            r for r in resources
            if self._any_policy_has_allow_all(
                iam.Role(r['RoleName']).attached_policies.all()
            )
        ]


@VpcEndpoint.filter_registry.register("target-all-resources")
class TargetAllResources(Filter):
    """Check if resource policy associated with the given resource applies does not apply to _all_ resources

        .. code-block:: yaml

        - name: vpc-s3-endpoint-to-all-bucket
          resource: aws.vpc-endpoint
          filters:
          - type: target-all-resources
        """

    schema = type_schema('target-all-resources')
    permissions = ('ec2:DescribeVpcEndpoint',)

    @staticmethod
    def _policy_document_applies_to_all(document):
        """
        Check if given document contains any violating statement.

        Violating statement consists of
        - Resource with Effect "allow"    - Resource shouldn't contain *; specific ARN must be specified
        - Resource with Effect "deny"     - can ignore this one as this doesn't fit our policy use case
        - NotResource with Effect "deny"  - deny all except mentioned in NotResource; NotResource shouldn't contain *
        - NotResource with Effect "allow" - reject right away

        :param document: PolicyDocument to scan for
        :return: True if any statement violates above rules else false
        """
        statements = document['Statement']
        if isinstance(statements, dict):  # there could be more than one statement
            statements = [statements]

        for stmt in statements:
            resources = stmt['NotResource'] if 'NotResource' in stmt else stmt['Resource']
            if isinstance(resources, str):
                resources = [resources]

            # special case for situations where effect is allow and NotResource is used
            # using it this way, one can allow access to _all_ other Resource except ones in NotResource
            if 'NotResource' in stmt and stmt['Effect'] == "Allow":
                return True

            for resource in resources:
                if resource == "*":
                    return True

                arn = arnparse(resource)
                if arn.service == "s3" and (arn.resource == "*" or '*' in arn.resource.split(sep='/')[0]):
                    return True
                else:  # to add support for other services add more clauses here
                    pass

        return False

    def process(self, resources, event=None):
        return [
            r for r in resources if self._policy_document_applies_to_all(
                json.loads(r['PolicyDocument'])
            )
        ]
