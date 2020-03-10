class Resource:
    def __init__(self, applier, manifest, **kwargs):
        self.applier = applier
        self.manifest = manifest
        self.values = kwargs.get("values", [])
        self.content = kwargs.get("content", None)
        self.hash = kwargs.get("hash", None)
        self.id = kwargs.get("id", None)
        self.requires = kwargs.get("requires", set())

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)
