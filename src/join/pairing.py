from collections import namedtuple
from typing import Iterable, Callable, Any

JoinedPair = namedtuple('JoinedPaired', ['a', 'b'])
JoinFactory = Callable[[Any, Any], Any]
JoinCondition = Callable[[Any, Any], bool]


class ConditionalPairing:

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
            or (lambda a, b: JoinedPair(a, b))

        self.condition: JoinCondition = cond\
            or (lambda a, b: True)

    def iterate(self):
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

    def iterate(self):
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



