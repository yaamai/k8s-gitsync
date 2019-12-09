class Resource:
    def __init__(self, applier, manifest, **kwargs):
        self.applier = applier
        self.manifest = manifest
        self.values = kwargs.get("values", [])
        self.id = kwargs.get("id", None)
        self.content = kwargs.get("content", None)
        self.required = kwargs.get("required", [])

    def __str__(self):
        return str(self.__dict__)
