from c7n.actions import BaseVariableSupportAction


def patch(registry, name):
    """
    Patches an existing action class and make it inherit from BaseVariableSupportAction
    to include all the goodies of variable support.

    In effect this method _generates_ a class which works similar to,

    ```
    class PatchX(BaseVariableSupportAction, X):
        def process_resource(self, data, resource):
            return X(data=data, manager=self.manager, log_dir=self.log_dir).process([resource])
    ```

    We need to inherit from an existing policy to inherit it's existing schema but we can't make a
    X.process(self, [resource]) call because most existing policies would read from self.data (which is not allowed
    for a class inheriting from BaseVariableSupportAction) and so, instead of that we create a new instance of X
    and invoke it's process(...) method using the pre-processed data object we receive in process_resource.

    Instead of having to define the above boilerplate each time, we implement this using type(...)
    and dynamic class generation. This should be sufficient for most of the cases, and if not, user are
    always free to provide their own patch class by overriding as done in above snippet.

    see also: https://stackoverflow.com/a/6581949/6611700
    """

    def make_process_resource(klass):
        """make_process_resource creates a method proxy that implements process_resource(...) abstract method
        """

        def process_resource(self, data, resource):
            return klass(data=data, manager=self.manager, log_dir=self.log_dir).process([resource])

        return process_resource

    klass = registry[name]
    patched_klass = type(f"Patch{klass.__name__}", (BaseVariableSupportAction, klass), {
        'process_resource': make_process_resource(klass)
    })

    return registry.register(name, patched_klass)  # override existing mapping in registry

# to patch existing action classes, call patch passing in the relevant registry and the registered action name
# for eg. to patch s3.SetPolicyStatement action, call => patch(s3.actions, "set-statements")
