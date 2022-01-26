from collections import defaultdict
from typing import Iterable, List, Any, Dict, Callable, Union, Generic, TypeVar, Hashable

T = TypeVar('T')
IndexableGetter = Callable[[Any], Hashable]


class Index(Generic[T]):

    def __init__(self, items: Iterable, index_by: Union[str, IndexableGetter]):
        self.items: Iterable[T] = items
        self.key_getter: IndexableGetter = Index.make_key_getter(index_by)
        self.map: Dict[Hashable, T] = self.index()
        self.index()

    def index(self) -> Dict[Hashable, T]:
        mapping = defaultdict(list)
        for item in self.items:
            mapping.get(self.key_getter(item)).append(item)
        return mapping

    def get_items(self):
        return self.items

    def find(self, key, *args):
        args = [key] + list(args)
        return self.map.get(*args)

    @staticmethod
    def make_key_getter(index_by: Union[str, IndexableGetter]) -> IndexableGetter:
        if isinstance(index_by, Callable):
            return index_by

        if isinstance(index_by, str):
            return lambda item: getattr(item, index_by)


class UniqueIndex(Index, Generic[T]):
    def index(self) -> Dict[Hashable, T]:
        return {
            self.key_getter(item): item
            for item in self.get_items()
        }


class ObjectIndices:

    def __init__(self, items: Iterable, unique: List[str] = None, indices: List[str] = None):
        self.items: Iterable[Any] = items
        self.indices: Dict[str, Index] = {}

        for unique_field in unique:
            # the name of the index will be the name of the attribute
            self.create_unique_index(unique_field, index_by=unique_field)

        for field in indices:
            # the name of the index will be the name of the attribute
            self.create_index(field, index_by=field)

    def create_unique_index(self, name: str, index_by: Union[str, IndexableGetter]):
        self.add_index(name, UniqueIndex(
            self.items, index_by=index_by
        ))

    def create_index(self, name: str, index_by: Union[str, IndexableGetter]):
        self.add_index(name, Index(
            self.items, index_by=index_by
        ))

    def add_index(self, name: str, index: Index):
        self.indices[name] = index

    def get_index(self, name: str) -> Index:
        return self.indices.get(name)

    def find(self, index_name: str, key: Any) -> Union[Any, List[Any]]:
        index: Index = self.indices.get(index_name)
        return index.find(key)


class DictIndices(ObjectIndices):

    def __init__(self, items: Iterable[Dict], unique: List[str] = None, indices: List[str] = None):
        super().__init__(items, unique, indices)

    def create_index(self, name: str, index_by: Union[str, IndexableGetter]):
        getter = DictIndices.make_index_getter(index_by)
        super().create_index(name, index_by=getter)

    def create_unique_index(self, name: str, index_by: Union[str, IndexableGetter]):
        getter = DictIndices.make_index_getter(index_by)
        super().create_unique_index(name, index_by=getter)

    @staticmethod
    def make_index_getter(index_by: Union[str, IndexableGetter]) -> IndexableGetter:
        if isinstance(index_by, str):
            return lambda item: item.get(index_by)

        return Index.make_key_getter(index_by)
