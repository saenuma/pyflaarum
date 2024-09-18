from . import objects

def name_validate(name: str) -> str:
  if "." in name or " " in name or "\t" in name or ":" in name or "\n" in name or "/" in name or "~" in name:
    raise ValueError("object name must not contain space, '.', ':', '/', ~ ")
  
  return name

def parse_table_structure_stmt(stmt: str) -> objects.Table:
  stmt = stmt.strip()
  if not stmt.startswith("table:"):
    raise Exception("Bad Statement: structure statements starts with 'table: '")
  
  ts = objects.Table()
  line1 = stmt.split("\n")[0]
  table_name = line1[len("table:"):].strip()
  name_validate(table_name)
  ts.table = table_name

  fields_begin_part = stmt.index("fields:")
  if fields_begin_part == -1:
    raise Exception("Bad Statement: structures statements must have a 'fields:' section.")
  
  fields_begin_part += len("fields:")
  fields_end_part = stmt[fields_begin_part:].index("::")
  if fields_end_part == -1:
    raise Exception("Bad Statement: fields section must end with a '::'.")
  
  fields_part = stmt[fields_begin_part : fields_begin_part+fields_end_part]
  fss = []

  for part in fields_part.splitlines():
    part = part.strip()
    if part == "":
      continue
    parts = part.split()
    if len(parts) < 2:
      raise Exception("Bad Statement: a fields definition must have a minimum of two words.")
    if parts[0] == "id" or parts[0] == "_version":
      raise Exception("Bad Statement: the fields 'id' and '_version' are automatically created. Hence can't be used.")
    
    name_validate(parts[0])
    if not parts[1] in ["int", "string", "text"]:
      raise Exception("Bad Statement: the field type '%s' is not allowed in flaarum." % parts[1])
    
    fs = objects.Field()
    fs.field_name = parts[0]
    fs.field_type = parts[1]
    if len(parts) > 2:
      for other_part in parts[2:] :
        if other_part == "required":
          fs.required = True
        elif other_part == "unique":
          fs.unique = True
        elif other_part == "nindex":
          fs.nindex = True
    
    fss.append(fs)
  
  ts.fields = fss

  try:
    fkey_part_begin = stmt.index("foreign_keys:")
    if fkey_part_begin != -1 :
      fkey_part_begin += len("foreign_keys:")
      fkey_part_end = stmt[fkey_part_begin:].index("::")
      if fkey_part_end == -1 :
        raise Exception("Bad Statement: a 'foreign_keys:' section must end with a '::'.")
      fkey_part = stmt[fkey_part_begin : fkey_part_begin+fkey_part_end]
      fks = []
      for part in fkey_part.splitlines():
        part = part.strip()
        if part == "":
          continue

        parts = part.split()
        if len(parts) != 3:
          raise Exception("Bad Statement: a line in a 'foreign_keys:' section must have three words.")

        fkobj = objects.FKey(parts[0], parts[1], parts[2])
        fks.append(fkobj)
      
      ts.foreign_keys = fks
  except:
    ts.foreign_keys = []

  return ts
  

def format_table_obj(table_obj: objects.Table) -> str:
  stmt = f"table: {table_obj.table_name}\n"
  stmt += "fields:\n"
  for field_struct in table_obj.fields:
      stmt += f"  {field_struct.field_name} {field_struct.field_type}"
      if field_struct.required:
          stmt += " required"
      if field_struct.unique:
          stmt += " unique"
      if field_struct.not_indexed:
          stmt += " nindex"
      stmt += "\n"
  stmt += "::\n"
  if table_obj.foreign_keys:
      stmt += "foreign_keys:\n"
      for fks in table_obj.foreign_keys:
          stmt += f"  {fks.field_name} {fks.pointed_table} {fks.on_delete}\n"
      stmt += "::\n"

  return stmt

