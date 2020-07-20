import json


class JsonDiffError(Exception):
    pass


class JsonDiff:
    """
        A representation of a 'diff' given a PATCH/POST body of the change.
        The diff is limited in the sense that it is just a representation of what the requested
        change was, NOT a representation of the "old" state of an item as well.
    """

    def __init__(self, body, item_type):
        self.body = body
        self.item_type = item_type
        self._representation = None

    def __repr__(self):
        """ Basic string reprsentation of the diff """
        return 'JsonDiff of item_type %s with delta:\n%s' % (self.item_type, self.body)

    def __eq__(self, other):
        """ Equals operator - for now just looks at item_type and body """
        return self.item_type == other.item_type and self.body == other.body

    def _build_representation(self):
        """ Helper method for __init__ that builds the 'representation' field
            XXX: what should this look like?
        """
        return None

    def serialize(self):
        """ Serializes into JSON """
        repr = {
            'body': self.body,
            'item_type': self.item_type,
            'representation': None  # TODO handle representation
        }
        return json.dumps(repr)

    @staticmethod
    def deserialize(obj):
        """ Deserializes into JSON """
        repr = json.loads(obj)
        return JsonDiff(repr['body'], repr['item_type'])  # TODO handle representation
