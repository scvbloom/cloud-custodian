from c7n import mu

"""
This file provides a set of patches for classes in c7n.mu package.
The patches enable us to tweak lambda provisioning and include c7x (and related code)
by default in the lambda code archive.
"""

# lambda execution entrypoint
# PolicyHandlerEntrypoint = """\
# from c7x import handler

# def run(event, context):
#     return handler.dispatch_event(event, context)

# """

# # override default handler function

# # @formatter:off
def patch_init(self, *args, **kwargs):
    """
    This routine patches the default mu.PolicyLambda.__init__()
    to include package 'c7x' by default in the code archive
    """
    old_init(self, *args, **kwargs)
    self.archive.add_modules(None, ('c7x', 'arnparse', 'argcomplete'))
    self.archive.add_contents('c7x_policy.py', PolicyHandlerEntrypoint)

# @formatter:on