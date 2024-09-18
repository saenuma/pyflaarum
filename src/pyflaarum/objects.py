from typing import List, Optional

class Field:
    def __init__(self):
      pass

class FKey:
    def __init__(self, field_name: str, pointed_table: str, on_delete: str):
        self.field_name = field_name
        self.pointed_table = pointed_table
        self.on_delete = on_delete  # expects one of "on_delete_restrict", "on_delete_delete"

class Table:
    def __init__(self):
        pass    
        # self.table_name = table_name
        # self.fields = fields
        # self.foreign_keys = foreign_keys

class Where:
    # def __init__(self, field_name: str, relation: str, field_value: str, joiner: str, field_values: List[str]):
    def __init__(self):
      pass
        # self.field_name = field_name
        # self.relation = relation  # eg. '=', '!=', '<', etc.
        # self.field_value = field_value
        # self.joiner = joiner  # one of 'and', 'or'
        # self.field_values = field_values  # for 'in' and 'nin' queries

class Stmt:
    def __init__(self):
      pass