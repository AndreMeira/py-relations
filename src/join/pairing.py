from collections import namedtuple
from typing import Iterable, Callable, Any, List, Dict

from relations.src.collection.index import UniqueIndex, Index

JoinedPair = namedtuple('JoinedPaired', ['a', 'b'])
JoinedElementToSubset = namedtuple('JoinedElementToSubset', ['element', 'subset'])
JoinFieldMapping = namedtuple('JoinFieldMapping', ['foreign_key', 'key'])
JoinFactory = Callable[[Any, Any], Any]
JoinCondition = Callable[[Any, Any], bool]



class ConditionalPairing:
    JOIN_THROUGH = JoinedPair

    def __init__(
        self,
        a: Iterable,
        b: Iterable,
        through: JoinFactory = None,
        cond: JoinCondition = None
    ):
        self._result: Iterable[JoinedPair, Any] = []
        self.a: Iterable = a
        self.b: Iterable = b
        self.through: JoinFactory = through\
            or (lambda a, b: self.JOIN_THROUGH(a, b))

        self.condition: JoinCondition = cond\
            or (lambda a, b: True)

    def iterate(self) -> List:
        return [(a, b) for a in self.a for b in self.b]

    def join(self) -> Iterable[JoinedPair, Any]:
        if not self._result:
            self._result = [
                self.through(a, b)
                for a, b in self.iterate()
                if self.condition(a, b)
            ]
        return self._result


class ZipPairing(ConditionalPairing):

    def __init__(self, a: Iterable, b: Iterable, through: JoinFactory = None):
        super().__init__(a, b, through, lambda a, b: True)

    def iterate(self) -> List:
        b = [item for item in self.b]
        a = [item for item in self.a]
        maxlength = min(len(a), len(b))

        return [
            (item, b.index(index))
            for index, item in enumerate(self.a[0:maxlength])
        ]


class UnrestrictedPairing(ConditionalPairing):

    def __init__(self, a: Iterable, b: Iterable, through: JoinFactory = None):
        super().__init__(a, b, through, lambda a, b: True)


class ElementToSubsetPairing(ConditionalPairing):
    JOIN_THROUGH = JoinedElementToSubset

    def join(self) -> Iterable[JoinedPair, Any]:
        return self.iterate()

    def iterate(self):
        return [
            self.through(element, self.subset_of(element))
            for element in self.a
        ]

    def subset_of(self, element):
        return [
            b for b in self.b
            if self.condition(element, b)
        ]


class ForeignKeyMappingPairing(ConditionalPairing):

    def __init__(self, a: Iterable, b: Iterable, mapping: Dict[str, str], through: JoinFactory = None):
        # ex: mapping = {"product_id":"id"}
        foreignkey, key = mapping.items()[0]
        self.mapping: JoinFieldMapping = JoinFieldMapping(
            foreign_key=foreignkey, key=key
        )
        self.index: UniqueIndex = UniqueIndex(b, key)
        cond: JoinCondition = lambda a, b: True
        super().__init__(a, b, cond=cond, through=through)

    def find_pair(self, el):
        try:
            key = getattr(el, self.mapping.foreign_key)
            self.index.find(key)
        except KeyError:
            return None

    def iterate(self) -> List:
        return [
            self.through(el, self.find_pair(el))
            for el in self.a
        ]


class ReferencedKeyMappingPairing(ElementToSubsetPairing):
    def __init__(self, a: Iterable, b: Iterable, mapping: Dict[str, str], through: JoinFactory = None):
        # ex: mapping = {"id":"product_id"}
        key, foreignkey = mapping.items()[0]
        self.mapping: JoinFieldMapping = JoinFieldMapping(
            foreign_key=foreignkey, key=key
        )
        self.index: Index = UniqueIndex(b, foreignkey)
        cond: JoinCondition = lambda a, b: True
        super().__init__(a, b, cond=cond, through=through)

    def subset_of(self, element):
        return self.index.find(
            getattr(element, self.mapping.key)
        )