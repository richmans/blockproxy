import json
BLOCK_MAGIC=3652501241

class Config:
  def __init__(self, configfile):
    self.configfile = configfile
    self.load()
  
  def load(self):
    config = open(self.configfile)
    self.data = json.load(config)
    config.close()
    
  def save(self):
    config = open(self.configfile, 'w')
    json.dump(self.data, config,  sort_keys=True,indent=2)
    config.close()

  def __getattr__(self, attr):
    if attr in self.data:
      return self.data[attr]
    else:
      raise AttributeError
class Block:
  def __init__(self, magic, length, data, fromFile, fileOffset):
    self.magic = magic
    self.length = length
    self.data = data
    self.fromFile = fromFile
    self.fileOffset = fileOffset
    
  def verify(self):
    if self.magic != BLOCK_MAGIC:
      print("No magic! %d != %d" % (self.magic, BLOCK_MAGIC))
      return False
      
    if self.length == 0 or self.length != len(self.data):
      print("Length is no good!")
      return False
    return True