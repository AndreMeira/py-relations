from typing import Iterable, Callable, Any, Generator, Type, Union


class GenericParentingRelation:
    # default is identity function
    CHILD_WRAPPER: Union[Type, Callable] = lambda x: x
    CHILDREN_COLLECTION_CLASS: Any = list

    def __init__(
        self,
        parents: Iterable = [],
        children: Iterable = [],
        owns: Callable[[Any, Any], bool] = lambda parent, child: False,
        through_attr: str = None
    ):
        self._parents: Iterable = parents
        self._children: Iterable = children
        self._owns = Callable[[Any, Any], bool] = owns
        self._through_attr: str = through_attr

    def set_children_collection_class(self, wrapper: Type):
        self.CHILDREN_COLLECTION_CLASS = wrapper

    def set_child_wrapper(self, wrapper: Union[Type, Callable]):
        self.CHILD_WRAPPER = wrapper

    def generate_children_of(self, parent) -> Generator[Any, None, None]:
        return (
            self.CHILD_WRAPPER(child) for child in self._children
            if self._owns(parent, child)
        )

    def get_children_of(self, parent: Any) -> type(CHILDREN_COLLECTION_CLASS):
        return self.CHILDREN_COLLECTION_CLASS(
            self.generate_children_of(parent)
        )

    def get_first_child_of(self, parent: Any) -> Any:
        return next(
            self.generate_children_of(parent)
        )


class GenericHasOne(GenericParentingRelation):

    def join(self):
        for parent in self._parents:
            child = self.get_first_child_of(parent)
            attr = self._through_attr or child.__class.__name
            setattr(parent, attr, child)


class GenericHasMany(GenericParentingRelation):
    CHILDREN_COLLECTION_CLASS = list

    def join(self):
        for parent in self._parents:
            children = self.get_children_of(parent)
            attr = self._through_attr
            setattr(parent, attr, children)


class GenericOneToMany(GenericParentingRelation):
    CHILDREN_COLLECTION_CLASS = list

    def join(self):
        for parent in self._parents:
            child = self.get_first_child_of(parent)
            attr = self._through_attr or child.__class.__name
            setattr(parent, attr, child)