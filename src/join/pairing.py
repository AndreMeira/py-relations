from collections import namedtuple
from typing import Iterable, Callable, Any, List, Dict, Union

from relations.src.collection.index import UniqueIndex, Index

JoinFactory = Callable[[Any, Any], Any]
JoinCondition = Callable[[Any, Any], bool]
JoinedPair = namedtuple('JoinedPaired', ['a', 'b'])
JoinFieldMapping = namedtuple('JoinFieldMapping', ['foreign_key', 'key'])
JoinedElementToSubset = namedtuple('JoinedElementToSubset', ['element', 'subset'])


def _fkmap(mapping: Dict[str, str]):
    foreignkey, key = mapping.items()[0]
    return JoinFieldMapping(
        key=key,
        foreign_key=foreignkey
    )


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

    def join(self) -> Iterable[Union[JoinedPair, Any]]:
        if not self._result:
            self._result = self.make_join()
        return self._result

    def make_join(self):
        return [
            self.through(a, b)
            for a, b in self.iterate()
            if self.condition(a, b)
        ]


class ZipPairing(ConditionalPairing):

    def __init__(
        self,
        a: Iterable,
        b: Iterable,
        through: JoinFactory = None
    ):
        a = [item for item in a]
        b = [item for item in b]
        super().__init__(
            a, b,
            through=through,
            cond=lambda a, b: True
        )

    def get_max_length(self):
        return min(len(self.a), len(self.b))

    def iterate(self) -> List:
        maxlength = self.get_max_length()

        return [
            (item, b.index(index))
            for index, item
            in enumerate(self.a[0:maxlength])
        ]


class UnrestrictedPairing(ConditionalPairing):

    def __init__(
        self,
        a: Iterable,
        b: Iterable,
        through: JoinFactory = None
    ):
        super().__init__(
            a, b,
            through=through
        )


class ElementToSubsetPairing(ConditionalPairing):

    JOIN_THROUGH = JoinedElementToSubset

    def join(self) -> Iterable[JoinedPair, Any]:
        if not self._result:
            self._result = self.iterate()
        return self._result

    def iterate(self):
        return [
            self.make_result_element(el)
            for el in self.a
        ]

    def make_result_element(self, element):
        return self.through(
            element,
            self.subset_of(element)
        )

    def subset_of(self, element):
        return [
            b for b in self.b
            if self.condition(element, b)
        ]


class ForeignKeyMappingPairing(ConditionalPairing):

    def __init__(
        self,
        a: Iterable,
        b: Iterable,
        mapping: JoinFieldMapping,
        through: JoinFactory = None
    ):
        self.mapping: JoinFieldMapping = mapping
        self.index: UniqueIndex = UniqueIndex(b, mapping.key)
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
    def __init__(
        self,
        a: Iterable,
        b: Iterable,
        mapping: JoinFieldMapping,
        through: JoinFactory = None
    ):
        self.mapping: JoinFieldMapping = mapping
        self.index: Index = UniqueIndex(b, mapping.foreign_key)
        cond: JoinCondition = lambda a, b: True
        super().__init__(a, b, cond=cond, through=through)

    def subset_of(self, element):
        return self.index.find(
            getattr(element, self.mapping.key)
        )