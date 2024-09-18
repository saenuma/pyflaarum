import requests
import json
from . import statements
from . import objects

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