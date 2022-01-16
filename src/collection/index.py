from typing import Iterable, List, Any, Dict, Callable


IndexableGetter = Callable[[Any]: Any]


class ObjectIndex:

    def __init__(self, items: Iterable, unique: List[str] = None, indices: List[str] = None):
        self.items: Iterable[Any] = items
        self.indices: Dict[str, Dict[Any, Any]] = {}

        for uniq in unique:
            # the name of the index will be the name of the attribute
            self.unique_index(uniq, uniq)

        for index in indices:
            # the name of the index will be the name of the attribute
            self.index(index, index)

    def unique_index(self, name: str, by: str = None, getter: IndexableGetter = None):
        getter: IndexableGetter = self._make_index_getter(by, getter)

        self.indices[name] = {
            getter(item): item
            for item in self.items
        }

    def index(self, name: str, by: str = None, getter: IndexableGetter = None):
        getter: IndexableGetter = self._make_index_getter(by, getter)

        self.indices[name] = {}

        for item in self.items:
            self.indices[name].setdefault(
                getter(item), []
            ).append(item)

    def _make_index_getter(self, by: str = None, getter: IndexableGetter = None) -> IndexableGetter:
        if getter is not None:
            return getter

        if by is not None:
            return lambda item: getattr(item, by)


class DictIndex(ObjectIndex):

    def __init__(self, items: Iterable[Dict], unique: List[str] = None, indices: List[str] = None):
        super().__init__(items, unique, indices)

    def _make_index_getter(self, by: str = None, getter: IndexableGetter = None) -> IndexableGetter:
        if getter is not None:
            return super()._make_index_getter(by, getter)

        if by is not None:
            return lambda item: item.get(by)
