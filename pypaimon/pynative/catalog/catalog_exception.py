

class ProcessSystemDatabaseException(ValueError):
    def __init__(self):
        super().__init__("Can't do operation on system database.")


class DatabaseNotExistException(ValueError):
    def __init__(self, name):
        super().__init__(f"Database {name} does not exist.")


class DatabaseAlreadyExistException(ValueError):
    def __init__(self, name):
        super().__init__(f"Database {name} already exists.")


class TableNotExistException(ValueError):
    def __init__(self, name):
        super().__init__(f"Table {name} does not exist.")


class TableAlreadyExistException(ValueError):
    def __init__(self, name):
        super().__init__(f"Table {name} already exists.")