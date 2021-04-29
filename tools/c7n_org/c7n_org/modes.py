from c7n.policy import execution, LambdaMode, PullMode
from c7n.utils import type_schema


@execution.register('periodic')
class PeriodicMode(LambdaMode, PullMode):
    """
    PeriodicMode is a custom policy execution mode that overrides the default PeriodicMode
    allowing us to deploy lambdas that execute the policy in a central account
    """

    POLICY_METRICS = ('ResourceCount', 'ResourceTime', 'ActionTime')

    # maintain compatibility with default Periodic mode
    schema = type_schema('periodic', schedule={'type': 'string'}, rinherit=LambdaMode.schema)

    def run(self, event, lambda_context):
        """Runs the policy whenever an event (scheduled / cron) arrives"""
        self.assume_member(event)  # see if we can / need to switch role into another account
        return PullMode.run(self)