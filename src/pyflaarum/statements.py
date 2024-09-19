from . import objects
import re
from typing import List

def name_validate(name: str) -> str:
  invalid_chars = (".", " ", "\t", ":", "\n", "/", "~")
  for ichar in invalid_chars:
    if ichar in name:
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



def special_split_line(line: str) -> List[str]:
  line = line.strip()
  splits = []
  tmp_word = ""
  index = 0

  while index < len(line):
    ch = line[index]
    if ch == "'":
        try:
          next_quote_index = line[index+1:].index("'")
          tmpWord = line[index+1 : index+next_quote_index+1]
          splits.append(tmp_word)
          tmp_word = ""
          index += next_quote_index + 2
          continue
        except:
           raise ValueError(f"The line \"{line}\" has a quote and no second quote.")
        
    elif ch in (" ", "\t"):
        if tmp_word:
            splits.append(tmp_word)
            tmp_word = ""
    else:
        tmp_word += ch
    index += 1

  if tmp_word:
      splits.append(tmp_word)

  return splits


def parse_where_sub_stmt(where_part: str) -> objects.Where:
  where_objs = []
  for part in where_part.splitlines():
    part = part.strip()
    if not part:
      continue

    parts = special_split_line(part)
    if len(parts) < 2:
        raise ValueError(f'The part "{part}" is not up to two words.')

    if not where_objs:
        where_obj = objects.Where()
        where_obj.field_name=parts[0]
        where_obj.relation=parts[1]
        if where_obj.relation == "in":
          where_obj.field_values = parts[2:]
        else:
          where_obj.field_value = parts[2]
    else:
      where_obj = objects.Where()
      where_obj.joiner = parts[0]
      where_obj.field_name=parts[1]
      where_obj.relation=parts[2]
      if where_obj.relation == "in":
        where_obj.field_values = parts[3:]
      else:
        where_obj.field_value = parts[3]

    where_objs.append(where_obj)

  found_and = False
  found_or = False
  for where_obj in where_objs:
    if where_obj.joiner == "and":
        found_and = True
    if where_obj.joiner == "or":
        found_or = True

    if found_and and found_or:
        raise ValueError("Cannot use both 'and' and 'or' in a where section.")

  return where_objs


def parse_search_stmt(stmt: str) -> objects.Stmt:
    stmt = stmt.strip()
    stmt_obj = objects.Stmt()
    
    for part in stmt.splitlines():
      part = part.strip()
      if not part:
          continue

      if part.startswith("table:"):
          parts = part[len("table:"):].split()
          if not parts:
              raise ValueError("The 'table:' part is required and accepts a table name followed by two optional words")
          stmt_obj.table = parts[0]
          if len(parts) > 1:
            for p in parts[1:]:
              if p == "expand":
                stmt_obj.expand = True
              elif p == "distinct":
                stmt_obj.distinct = True
      elif part.startswith("fields:"):
        stmt_obj.fields = part[len("fields:"):].split()
      elif part.startswith("start_index:"):
        start_index_str = part[len("start_index:"):].strip()
        try:
            stmt_obj.start_index = int(start_index_str)
        except ValueError:
            raise ValueError(f"The data '{start_index_str}' for the 'start_index:' part is not a number.")
      elif part.startswith("limit:"):
          limit_str = part[len("limit:"):].strip()
          try:
              stmt_obj.limit = int(limit_str)
          except ValueError:
              raise ValueError(f"The data '{limit_str}' for the 'limit:' part is not a number.")
      elif part.startswith("order_by:"):
        parts = part[len("order_by:"):].split()
        if len(parts) != 2:
          raise ValueError("The words for 'order_by:' part must be two: a field and either of 'asc' or 'desc'")
        stmt_obj.order_by = parts[0]
        if parts[1] not in ["asc", "desc"]:
          raise ValueError(f"The order direction must be either of 'asc' or 'desc'. Instead found '{parts[1]}'")
        stmt_obj.order_direction = parts[1]

    have_multi = "joiner:" in stmt
    if have_multi:
      stmt = stmt.strip()
      stmt_joiner = ""
      for part in stmt.split('\n'):
        part = part.strip()
        if not part:
          continue

        if part.startswith("joiner:"):
          opt = part[len("joiner:"):].strip()
          if opt not in ["and", "or"]:
            raise ValueError("joiner only accepts either 'and' or 'or'")
          else:
            stmt_joiner = opt
          break

      where_opts = []
      # where1
      where1_part_begin = stmt.index("where1:")
      where1_part_end = stmt[where1_part_begin:].index("::")
      if where1_part_end == -1:
        raise ValueError("Every where section must end with '::'")
      where1_part = stmt[where1_part_begin+len("where1:"):where1_part_begin+where1_part_end]
      where1_structs = parse_where_sub_stmt(where1_part)
      where_opts.append(where1_structs)

      # where2
      where2_part_begin = stmt.index("where2:")
      if where2_part_begin == -1:
        raise ValueError("A statement with 'final_stmt:' must have 'where1:' and 'where2:' sections")

      where2_part_end = stmt[where2_part_begin:].index("::")
      if where2_part_end == -1:
        raise ValueError("Every where section must end with '::'")
      where2_part = stmt[where2_part_begin+len("where2:"):where2_part_begin+where2_part_end]
      where2_structs = parse_where_sub_stmt(where2_part)
      where_opts.append(where2_structs)

      where3_part_begin = stmt.find("where3:")
      if where3_part_begin != -1:
        where3_part_end = stmt[where3_part_begin:].index("::")
        if where3_part_end == -1:
          raise ValueError("Every where section must end with '::'")
        where3_part = stmt[where3_part_begin+len("where3:"):where3_part_begin+where3_part_end]
        where3_structs = parse_where_sub_stmt(where3_part)
        where_opts.append(where3_structs)

      where4_part_begin = stmt.find("where4:")
      if where4_part_begin != -1:
        where4_part_end = stmt[where4_part_begin:].index("::")
        if where4_part_end == -1:
          raise ValueError("Every where section must end with '::'")
        where4_part = stmt[where4_part_begin+len("where4:"):where4_part_begin+where4_part_end]
        where4_structs = parse_where_sub_stmt(where4_part)
        where_opts.append(where4_structs)

      stmt_obj.Multi = True
      stmt_obj.MultiWhereOptions = where_opts
      stmt_obj.Joiner = stmt_joiner

    else:
      where_part_begin = stmt.find("where:")
      if where_part_begin != -1:
        where_part = stmt[where_part_begin+len("where:"):]
        where_objs = parse_where_sub_stmt(where_part)

        stmt_obj.Multi = False
        stmt_obj.WhereOptions = where_objs

    return stmt_obj
