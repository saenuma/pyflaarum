import requests

requests.packages.urllib3.disable_warnings()

class flaa_error(Exception):
  def __init__(self, code, msg):
    self.msg = msg
    self.code = code
    self.message = "Error Code: " + str(code) + "\n" + msg


class flaacl:
  def __init__(self, ip, key_str, proj_name, port=22318):
    self.ip = ip
    self.key_str = key_str
    self.proj_name = proj_name
    self.port = port
    self.addr = "https://" + self.ip + ":" + str(self.port) + "/"

  def ping(self):
    data = {"key-str": self.key_str}
    ping_r = requests.post(self.addr + "is-flaarum", verify=False)

    if ping_r.status_code != requests.codes.ok:
      print(ping_r.text)
      raise flaa_error(10, "Unexpected Error in confirming that the server is a flaarum store.")
      