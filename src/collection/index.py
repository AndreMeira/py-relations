from collections import defaultdict
from typing import Iterable, List, Any, Dict, Callable, Union

IndexableGetter = Callable[[Any]: Any]


# @todo use Gerenic[T] for typing
class Index:

    def __init__(self, items: Iterable, by: str = None, getter: IndexableGetter = None):
        self.items: Iterable[Any] = items
        self.key_getter: IndexableGetter = Index.make_key_getter(by, getter)
        self.map: Dict[Any, Any] = self.index()
        self.index()

    def index(self) -> Dict[Any, Any]:
        mapping = defaultdict(list)
        for item in self.items:
            mapping.get(self.key_getter(item)).append(item)
        return mapping

    def get_items(self):
        return self.items

    def find(self, key, *args):
        args = [key] + args
        return self.index.get(*args)

    @staticmethod
    def make_key_getter(by: str = None, getter: IndexableGetter = None) -> IndexableGetter:
        if getter is not None:
            return getter

        if by is not None:
            return lambda item: getattr(item, by)


class UniqueIndex(Index):
    def index(self) -> Dict[Any, Any]:
        return {
            self.key_getter(item): item
            for item in self.get_items()
        }


class ObjectIndices:

    def __init__(self, items: Iterable, unique: List[str] = None, indices: List[str] = None):
        self.items: Iterable[Any] = items
        self.indices: Dict[str, Index] = {}

        for uniq in unique:
            # the name of the index will be the name of the attribute
            self.unique_index(uniq, by=uniq)

        for index in indices:
            # the name of the index will be the name of the attribute
            self.index(index, by=index)

    def create_unique_index(self, name: str, by: str = None, getter: IndexableGetter = None):
        self.add_index(name, UniqueIndex(
            self.items, by=by, getter=getter
        ))

    def create_index(self, name: str, by: str = None, getter: IndexableGetter = None):
        self.add_index(name, Index(
            self.items, by=by, getter=getter
        ))

    def add_index(self, name:str, index: Index):
        self.indices[name] = index

    def get_index(self, name: str) -> Index:
        return self.indices.get(name)

    def find(self, index_name: str, key: Any) -> Union[Any, List[Any]]:
        index: Index = self.index(index_name)
        return index.find(key)


class DictIndices(ObjectIndices):

    def __init__(self, items: Iterable[Dict], unique: List[str] = None, indices: List[str] = None):
        super().__init__(items, unique, indices)

    def create_index(self, name: str, by: str = None, getter: IndexableGetter = None):
        getter = DictIndices.make_index_getter(by, getter)
        super().create_index(name, getter=getter)

    def create_unique_index(self, name: str, by: str = None, getter: IndexableGetter = None):
        getter = DictIndices.make_index_getter(by, getter)
        super().create_unique_index(name, getter=getter)

    @staticmethod
    def make_index_getter(by: str = None, getter: IndexableGetter = None) -> IndexableGetter:
        if by is not None:
            return lambda item: item.get(by)
        return Index.make_key_getter(by, getter)