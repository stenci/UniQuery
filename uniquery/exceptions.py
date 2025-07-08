class MissingPrimaryKey(Exception):
    pass


class MultiplePrimaryKeys(Exception):
    pass


class MissingId(Exception):
    pass


class WrongNumberOfColumnsInQuery(Exception):
    pass


class UniQueryModelNotFoundError(Exception):
    pass


class WrongDialect(Exception):
    pass


class RenamedAttributeNotFound(Exception):
    pass
