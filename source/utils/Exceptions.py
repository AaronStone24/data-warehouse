class TableExistsError(Exception):
    pass

class TableDoesNotExistError(Exception):
    pass

class MappingError(Exception):
    def __init__(self, oltp_table_name, olap_table_name, message):
        self.oltp_table_name = oltp_table_name
        self.olap_table_name = olap_table_name
        self.message = message
        super().__init__(self.message)

class MergeError(Exception):
    def __init__(self, merge_proc_name, message):
        self.merge_proc_name = merge_proc_name
        self.message = message
        super().__init__(self.message)

class SqlError(Exception):
    def __init__(self, return_code, message):
        self.return_code = return_code
        self.message = message
        super().__init__(self.message)