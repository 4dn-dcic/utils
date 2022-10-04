import json


class DiffManager:
    """
    ******************************
    NOTE: THIS ENTIRE CLASS IS EXPERIMENTAL AND SHOULD NOT BE RELIED UPON UNTIL THIS MESSAGE IS REMOVED.
    ******************************
    This class encapsulates a number of comparison operations according to a specified set of style conventions.
    """

    VALID_STYLES = {'javascript', 'python', 'list'}

    class UnknownStyle(Exception):

        def __init__(self, style):
            self.style = style
            super().__init__("%s is not a known style." % style)

    def __init__(self, style='javascript', sort_by_change_type=False,
                 label=None):
        """
        Args:
            style str: one of javascript, python, or list. Controls whether nesting is a.b or a["b"] or ('a', 'b').
            sort_by_change bool: True if results should be sorted by change type, False if by item label
            label: an initial prefix label
        """

        self.style = style
        self.sort_by_change_type = sort_by_change_type
        self.label = label

    def _merge_label_key(self, label, key):
        """Merges a given dictionary key with a given recursively-accumulated label path-so-far."""
        if label is None:
            return (key,) if self.style == 'list' else key
        elif self.style == 'javascript':
            return "{label}.{key}".format(label=label, key=key)
        elif self.style == 'python':
            return '{label}[{key}]'.format(label=label, key=json.dumps(key))
        elif self.style == 'list':
            return label + (key,)
        else:
            raise self.UnknownStyle(self.style)

    def _merge_label_elem(self, label, pos, _omit_subscripts=False):
        """Merges a given element index with a given recursively-accumulated label path-so-far."""
        if label is None:
            if _omit_subscripts:
                return label
            else:
                return (pos,) if self.style == 'list' else "[{pos}]".format(pos=pos)
        elif self.style in {'javascript', 'python'}:
            return label if _omit_subscripts else "{label}[{pos}]".format(label=label, pos=pos)
        elif self.style == 'list':
            return label if _omit_subscripts else label + (pos,)
        else:
            raise self.UnknownStyle(self.style)

    CONSTANTS_MAP = {None: 'null', True: 'true', False: 'false'}

    def unroll(self, item, normalizer=None, _omit_subscripts=False, _omit_empty_containers=True):
        """
        Unrolls a JSON structure (a multi-level Python dictionary) into a (flat) dictionary of dotted keys.
        The keys in the dictionary depend on the DiffManager's style attribute.
        e.g.,
        {"a": {"b": 1, "c": 2}, "b": 3} => {"a.b": 1, "a.c": 2, "b": 3}               # javascript style
        {"a": {"b": 1, "c": 2}, "b": 3} => {'a["b"]': 1, 'a["c"]': 2, 'b': 3}         # python style
        {"a": {"b": 1, "c": 2}, "b": 3} => {('a', 'b'): 1, ('a', 'c'): 2, ('b',): 3}  # list style
        """

        result = {}

        def traverse(item, result, label):
            if normalizer:
                item = normalizer(label=label, item=item)
            if (item or _omit_empty_containers) and isinstance(item, dict):
                for k, v in item.items():
                    traverse(v, result, self._merge_label_key(label=label, key=k))
            elif (item or _omit_empty_containers) and isinstance(item, list):
                for i, elem in enumerate(item):
                    # TODO: Would it be simpler to just omit subscripts here?
                    traverse(elem, result,
                             self._merge_label_elem(label=label, pos=i, _omit_subscripts=_omit_subscripts))
            else:
                result[label] = True if _omit_subscripts else item

        traverse(item, result, self.label)

        return result

    def diffs(self, item1, item2, include_mappings=False, normalizer=None,
              _omit_subscripts=False, _omit_empty_containers=True):
        """
        Args:
            item1: a python dictionary (representing its JSON equivalent)
            item2: a python dictionary (representing its JSON equivalent)
            include_mappings (bool): a boolean indicating whether old/new mappings should be returned
            normalizer: an optional function that normalizes the object before comparison
            _omit_subscripts: Used INTERNALLY ONLY by .patch_diffs to cause some detail to be elided.
            _omit_empty_containers: whether to treat "foo": [] or "foo": {} the same as if it were missing
        Returns:
            a dictionary of with keys "added", "changed", "same", and "removed" containing names of keys of each kind.
            If include_mappings=True, then "old" and "new" will be among the keys.
        """

        d1 = self.unroll(item1, normalizer=normalizer,
                         _omit_subscripts=_omit_subscripts, _omit_empty_containers=_omit_empty_containers)
        d2 = self.unroll(item2, normalizer=normalizer,
                         _omit_subscripts=_omit_subscripts, _omit_empty_containers=_omit_empty_containers)
        return self._diffs(d1, d2, include_mappings=include_mappings)

    @classmethod
    def _diffs(cls, d1, d2, include_mappings=False):
        removed = []
        added = []
        same = []
        changed = []
        for k1 in d1.keys():
            if k1 not in d2:
                removed.append(k1)
            else:
                if d1[k1] == d2[k1]:
                    same.append(k1)
                else:
                    changed.append(k1)
        for k2 in d2.keys():
            if k2 not in d1:
                added.append(k2)
        res = {}
        if removed:
            res['removed'] = removed
        if changed:
            res['changed'] = changed
        if same:
            res['same'] = same
        if added:
            res['added'] = added
        if include_mappings:
            res['old'] = d1
            res['new'] = d2
        return res

    def patch_diffs(self, item):
        """
        Returns a list of keys for would-be-affected properties if item were a patch request for a piece of JSON.
        """
        result = self.diffs({}, item, _omit_subscripts=True, _omit_empty_containers=False)
        return sorted(result.get('added', []))

    def _maybe_sorted(self, items, for_change_type):
        if self.sort_by_change_type:
            return sorted(items) if for_change_type else items
        else:
            return items if for_change_type else sorted(items)

    def comparison(self, item1, item2, normalizer=None):
        """
        Returns a description of the changes between two JSON items (represented as Python dictionaries).
        Each line is a string of the form "<key-path>: <old> => <new>".
        For additions, the <old> will be absent.
        For removals, the <new> will be absent.
        Items remaining the same will not be listed.
        """
        d1 = self.unroll(item1, normalizer=normalizer)
        d2 = self.unroll(item2, normalizer=normalizer)
        diffs = self._diffs(d1, d2)
        result = []

        subresult = []
        for k in diffs.get('removed', []):
            subresult.append("{label} : {value1} =>".format(label=k, value1=json.dumps(d1[k])))
        result = result + self._maybe_sorted(subresult, for_change_type=True)

        subresult = []
        for k in diffs.get('changed', []):
            subresult.append("{label} : {value1} => {value2}".format(label=k,
                                                                     value1=json.dumps(d1[k]),
                                                                     value2=json.dumps(d2[k])))
        result = result + self._maybe_sorted(subresult, for_change_type=True)

        subresult = []
        for k in diffs.get('added', []):
            subresult.append("{label} : => {value2}".format(label=k, value2=json.dumps(d2[k])))
        result = result + self._maybe_sorted(subresult, for_change_type=True)

        return self._maybe_sorted(result, for_change_type=False)
