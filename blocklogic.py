import json
BLOCK_MAGIC=3652501241

class Config:
  def __init__(self, configfile):
    self.data = {}
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
    if attr in self.__dict__:
      return super().__setattr__(attr)
    elif attr in self.data:
      return self.data[attr]
  
  def __setattr__(self, attr, value):
    if attr in self.__dict__ or attr in ['data', 'configfile']:
      return super().__setattr__(attr, value)
    if attr in self.data:
      self.data[attr] = value
      
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