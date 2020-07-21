import json


class DiffManager:

    def __init__(self, style='javascript'):
        self.style = style

    def merge_label_key(self, label, key):
        if self.style == 'javascript':
            return "{label}.{key}".format(label=label, key=key)
        elif self.style == 'python':
            return '{label}[{key}]'.format(label=label, key=self._q(key))

    def merge_label_elem(self, label, pos):
        return "{label}[{pos}]".format(label=label, pos=pos)

    CONSTANTS_MAP = {None: 'null', True: 'true', False: 'false'}

    def unroll(self, item, *, result=None, label="item"):
        if result is None:
            result = {}
        if isinstance(item, dict):
            for k, v in item.items():
                self.unroll(v, result=result, label=self.merge_label_key(label=label, key=k))
        elif isinstance(item, list):
            for i, elem in enumerate(item):
                self.unroll(elem, result=result, label=self.merge_label_elem(label=label, pos=i))
        else:
            result[label] = item
        return result

    def diffs(self, item1, item2):
        d1 = self.unroll(item1)
        d2 = self.unroll(item2)
        return self._diffs(d1, d2)

    def _diffs(self, d1, d2):
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
        return res

    def comparison(self, item1, item2):
        d1 = self.unroll(item1)
        d2 = self.unroll(item2)
        diffs = self._diffs(d1, d2)
        result = []
        for k in diffs.get('removed', []):
            result.append("{label} : {value1} =>".format(label=k, value1=self._q(d1[k])))
        for k in diffs.get('changed', []):
            result.append("{label} : {value1} => {value2}".format(label=k,
                                                                  value1=self._q(d1[k]), value2=self._q(d2[k])))
        for k in diffs.get('added', []):
            result.append("{label} : => {value2}".format(label=k, value2=self._q(d2[k])))
        return result

    def _q(self, datum):
        return json.dumps(datum)
