import requests
import json
from . import statements
from . import objects
from typing import List, Dict

requests.packages.urllib3.disable_warnings()

class flaa_error(Exception):
  def __init__(self, code: int, msg: str):
    self.msg = msg
    self.code = code
    self.message = "Error Code: " + str(code) + "\n" + msg


class flaacl:
  def __init__(self, ip: str, key_str: str, proj: str, port=22318):
    self.ip = ip
    self.key_str = key_str
    self.proj = proj
    self.port = port
    self.addr = "https://" + self.ip + ":" + str(self.port) + "/"

  def ping(self) -> None:
    data = {"key-str": self.key_str}
    robj = requests.post(self.addr + "is-flaarum", data=data, verify=False)

    if robj.status_code != requests.codes.ok:
      raise flaa_error(10, "Unexpected Error in confirming that the server is a flaarum store.")
  
  def create_project(self, proj: str) -> None:
    data = {"key-str": self.key_str}
    rurl = self.addr + "create-project/" + proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(10, robj.text)
  
  def delete_project(self, proj: str) -> None:
    data = {"key-str": self.key_str}
    rurl = self.addr + "delete-project/" + proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(10, robj.text)
  
  def list_projects(self):
    data = {"key-str": self.key_str}
    robj = requests.post(self.addr + "list-projects", data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      return json.loads(robj.text)
    else:
      raise flaa_error(11, robj.text)
  
  def rename_project(self, proj: str, new_proj: str):
    data = {"key-str": self.key_str}
    rurl = self.addr +"rename-project/"+proj+"/"+new_proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(11, robj.text)
  
  def create_table(self, stmt: str):
    data = {"key-str": self.key_str, "stmt": stmt}
    rurl = self.addr +"create-table/"+ self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(11, robj.text)
  
  def update_table_structure(self, stmt: str):
    data = {"key-str": self.key_str, "stmt": stmt}
    rurl = self.addr +"update-table-structure/"+ self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(11, robj.text)
  
  def list_tables(self):
    data = {"key-str": self.key_str}
    rurl = self.addr + "list-tables/" + self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      return json.loads(robj.text)
    else:
      raise flaa_error(11, robj.text)
  
  def current_table_version_num(self, table: str) -> int:
    data = {"key-str": self.key_str}
    rurl = "%sget-current-version-num/%s/%s" % (self.addr, self.proj, table)
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      return int(robj.text.strip())
    else:
      raise flaa_error(11, robj.text)
  
  def table_structure(self, table: str, version: int) -> str:
    data = {"key-str": self.key_str}
    rurl = "%sget-table-structure/%s/%s/%d" % (self.addr, self.proj, table, version)
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      return robj.text
    else:
      raise flaa_error(11, robj.text)
  
  def table_structure_parsed(self, table: str, version: int) -> objects.Table:
    stmt = self.table_structure(table, version)
    return statements.parse_table_structure_stmt(stmt)
  
  def current_table_structure_parsed(self, table: str) -> objects.Table:
    version = self.current_table_version_num(table)
    stmt = self.table_structure(table, version)
    return statements.parse_table_structure_stmt(stmt)
  
  def create_or_update_table(self, stmt: str) -> None:
    tables = self.list_tables()
    table_obj = statements.parse_table_structure_stmt(stmt)
    if not table_obj.table_name in tables:
      self.create_table(stmt)
    else:
      version = self.current_table_version_num(table_obj.table_name)
      old_stmt = self.table_structure(table_obj.table_name, version)
      if old_stmt != statements.format_table_obj(table_obj):
        self.update_table_structure(stmt)

  def delete_table(self, table: str) -> None:
    data = {"key-str": self.key_str}
    rurl = self.addr + "delete-table/" + self.proj + "/" + table
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(10, robj.text)
  
  def insert_row(self, table: str, to_insert: dict) -> int:
    data = {"key-str": self.key_str}
    for key, value in to_insert.items():
      data[key] = value
    
    table_obj = self.current_table_structure_parsed(table)
    fields = []
    for field_obj in table_obj.fields:
      fields.append(field_obj.field_name)
    
    for key, value in to_insert:
      if key == "id" or key == "_version":
        msg = "The field '%s' would be generated. Please remove." % (key)
        raise flaa_error(20, msg)
      
      if not key in fields:
        msg = "The field '%s' is not part of this table structure" % (key)
        raise flaa_error(20, msg)
    
    for field_obj in table_obj.fields:
      if field_obj.field_type == "string":
        v = to_insert.get(field_obj.field_name, "")

        if len(v) > 200:
          msg = "The value '%s' to field '%s' is longer than 200 characters" % (v, field_obj.field_name)
          raise flaa_error(24, msg)
        
        if "\n" in v or "\r\n" in v:
          msg = "The value of field '%s' contains new line." % field_obj.field_name
      
      if field_obj.field_type == "int" and field_obj.field_type in to_insert:
        v = to_insert[field_obj.field_name]
        if type(v) != str:
          try:
            int(v)
          except:
            msg = "The value '%s' to field '%s' is not of type 'int'" % (v, field_obj.field_name)
            raise flaa_error(24, msg)

      if not field_obj.field_name in to_insert and hasattr(field_obj, "required") and field_obj.required:
        msg = "The field '%s' is required." % field_obj.field_name
        raise flaa_error(20, msg)

    rurl = "%sinsert-row/%s/%s" % (self.addr, self.proj, table)
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      return int(robj.text.strip())
    elif robj.status_code == requests.codes.bad_request:
      if robj.text.strip().startswith("UE:"):
        raise flaa_error(21, robj.text.strip()[3:])
      elif robj.text.strip().startswith("FKE:"):
        raise flaa_error(23, robj.text.strip()[4:])
      else:
        raise flaa_error(20, robj.text.strip())
    else:
      raise flaa_error(11, robj.text)
    
  def __parserow(self, obj: Dict[str, str]) -> Dict[str, any]:
    pass

  def search(self, stmt: str) -> List[Dict[str, str]]:
    data = {"key-str": self.key_str, "stmt": stmt}
    try:
      statements.parse_search_stmt(stmt)
    except Exception as e:
      raise flaa_error(12, e.message)
    
    rurl = self.addr + "search-table/" + self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      tmpdict = json.loads(robj.text)
      return tmpdict
    else:
      raise flaa_error(11, robj.text)

  def search_for_one(self, stmt: str) -> Dict[str, str]:
    data = {"key-str": self.key_str, "stmt": stmt, "query-one": "t"}
    try:
      statements.parse_search_stmt(stmt)
    except Exception as e:
      raise flaa_error(12, e.message)
    
    rurl = self.addr + "search-table/" + self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      tmpdict = json.loads(robj.text)
      return tmpdict
    else:
      raise flaa_error(11, robj.text)
  
  def delete_rows(self, stmt: str) -> None:
    data = {"key-str": self.key_str, "stmt": stmt}
    try:
      statements.parse_search_stmt(stmt)
    except Exception as e:
      raise flaa_error(12, e.message)
    
    rurl = self.addr + "delete-rows/" + self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(10, robj.text)
  
  def count_rows(self, stmt: str) -> int:
    data = {"key-str": self.key_str, "stmt": stmt}
    try:
      statements.parse_search_stmt(stmt)
    except Exception as e:
      raise flaa_error(12, e.message)
     
    rurl = self.addr + "count-rows/" + self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      return int(robj.text.strip())
    else:
      raise flaa_error(10, robj.text)

  def all_rows_count(self, table: str) -> int:
    data = {"key-str": self.key_str}
    rurl = "%sall-rows-count/%s/%s" % ( self.addr, self.proj, table)
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code == requests.codes.ok:
      return int(robj.text.strip())
    else:
      raise flaa_error(10, robj.text)
    
  def update_rows(self, stmt: str, to_update: Dict[str, any]) -> None:
    data = {"key-str": self.key_str, "stmt": stmt}
    try:
      statements.parse_search_stmt(stmt)
    except Exception as e:
      raise flaa_error(12, e.message)
       
    keys = to_update.keys()
    for i in range(len(keys)):
      data["set%d_k" % i+1] = keys[i]
      data["set%d_v" % i+1] = to_update[keys[i]]
    
    rurl = self.addr + "update-rows/" + self.proj
    robj = requests.post(rurl, data=data, verify=False)
    if robj.status_code != requests.codes.ok:
      raise flaa_error(10, robj.text)
      