#!/usr/bin/env python
import json
import time
import os
import struct
import pip
try:
  import pika
except:
  pip.main(['install', "pika"])
  import pika
BLOCK_MAGIC=3652501241
dirPath = os.path.dirname(os.path.realpath(__file__))
configfile = os.path.join(dirPath, 'blockproxy.json')

class Config:
  def __init__(self):
    self.load()
  
  def load(self):
    config = open(configfile)
    data = json.load(config)
    self.startFile = data["startFile"]
    self.startByte = data["startByte"]
    self.blockDir = data["blockDir"]
    self.rabbitHost = data["rabbit"]["host"]
    self.rabbitUser = data["rabbit"]["user"]
    self.rabbitPassword = data["rabbit"]["password"]
    self.rabbitVirtualHost = data["rabbit"]["virtualHost"]
    self.rabbitExchange = data["rabbit"]["exchange"]
    config.close()
    
  def save(self):
    data = {
      "startFile": self.startFile,
      "startByte": self.startByte,
      "blockDir": self.blockDir,
      "rabbit": {
        "host": self.rabbitHost,
        "user": self.rabbitUser,
        "password": self.rabbitPassword,
        "virtualHost": self.rabbitVirtualHost,   
        "exchange": self.rabbitExchange 
      }
    }
    config = open(configfile, 'w')
    json.dump(data, config)
    config.close()

class BlockSender:
  def __init__(self, config):
    self.config = config
    self.start()
  
  def start(self):
    try:
      credentials = pika.PlainCredentials(self.config.rabbitUser, self.config.rabbitPassword)
      self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                     self.config.rabbitHost,
                     5672,
                     self.config.rabbitVirtualHost,
                     credentials))
      self.channel = self.connection.channel()
    except Exception as e:
      print(e)
      
  def publishBlock(self, block):
    self.channel.basic_publish(exchange=self.config.rabbitExchange,
                          routing_key='',
                          body=block.data)
  def close(self):
    self.connection.close()

class Block:
  def __init__(self, magic, length, data):
    self.magic = magic
    self.length = length
    self.data = data
    
  def verify(self):
    if self.magic != BLOCK_MAGIC:
      print("No magic! %d != %d" % (self.magic, BLOCK_MAGIC))
      return False
      
    if self.length == 0 or self.length != len(self.data):
      print("Length is no good!")
      return False
    return True
      
class BlockDir:
  def __init__(self, config, number, offset):
    self.number = number
    self.blocks = 0
    self.config = config
    self.open(offset)
    
  def open(self, offset=0):
    self.file = None
    self.offset = offset
    print("Opening file %d" % self.number)
    filename = "blk%05d.dat" % self.number
    fullpath = os.path.join(self.config.blockDir, filename)
    if not os.path.isfile(fullpath):
      return False
    self.file = open(fullpath, 'rb')
    self.file.seek(self.offset)
    self.blocks = 0
 
  def close(self):
    print("Closing file %d, read %d blocks" % (self.number, self.blocks))
    if self.file != None:
      self.file.close()
  
  def readInt(self):
    if self.file == None:
      return None
    inputData = self.file.read(4)
    try:
      return struct.unpack("<I", inputData)[0]
    except:
      return None
      
  def nextFile(self):
    self.close()
    self.number += 1
    self.open()
    
  def readBlock(self):
    magic = self.readInt()
    if magic == None:
      self.nextFile()
      magic = self.readInt()
    if magic == None or magic == 0:
      return None
    length = self.readInt()
    data = self.file.read(length)
    self.blocks += 1
    self.offset = self.file.tell()
    return Block(magic, length, data)
    
class BlockProxy:
  def __init__(self):
    self.config = Config()
    self.currentFile = self.config.startFile
    self.currentOffset = self.config.startByte
    self.sender = BlockSender(self.config)
    self.blocksSinceSave = 0
  
  def updateConfig(self):
    self.config.startFile = self.currentFile
    self.config.startByte = self.currentOffset
    self.config.save()
    
  def handleBlocks(self):
    blockDir = BlockDir(self.config, self.currentFile, self.currentOffset)
    while True:
      block = blockDir.readBlock()
      if(block == None): break
      result = block.verify()
      if(result == False): break
      self.sender.publishBlock(block)
      self.currentFile = blockDir.number
      self.currentOffset = blockDir.offset
      self.blocksSinceSave += 1
      if self.blocksSinceSave > 1000:
        self.updateConfig()
        self.blocksSinceSave = 0
    blockDir.close()
    self.updateConfig()
    
  def start(self):
    while True:
      self.handleBlocks()
      time.sleep(10)

BlockProxy().start()