#!/usr/bin/env python
import time
import os
import struct
import pip
from blocklogic import *
try:
  import pika
except:
  pip.main(['install', "pika"])
  import pika

dirPath = os.path.dirname(os.path.realpath(__file__))
configfile = os.path.join(dirPath, 'blockwriter.json')

class BlockReceiver:
  def __init__(self, config):
    self.config = config
    self.started = False
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
      self.channel.queue_declare(queue=self.config.rabbitQueue)
    except Exception as e:
      return False
    self.started = True
    return True
  
  
  def receiveBlock(self):
    _, _, body = self.channel.basic_get(self.config.rabbitQueue, no_ack=True)
    if body == None: return None
    fromFile, fileOffset = struct.unpack("<II", body[0:8]) 
    body = body[8:]
    return Block(BLOCK_MAGIC, len(body), body, fromFile, fileOffset)
    
  def close(self):
    self.connection.close()
      
class BlockDirWriter:
  def __init__(self, config):
    self.config = config
    
  def open(self, number, offset=0):
    self.file = None
    print("Opening file %d" % number)
    filename = "blk%05d.dat" % number
    fullpath = os.path.join(self.config.blockDir, filename)
    # mode r+b doesnt create a file, so lets doe that
    if not os.path.isfile(fullpath):
      open(fullpath, 'a').close()
    self.file = open(fullpath, 'r+b')
    self.file.seek(offset)
 
  def close(self):
    print("Closing file")
    if self.file != None:
      self.file.close()
  
  def writeBlock(self, block):
    self.open(block.fromFile, block.fileOffset)
    header = struct.pack("<II", block.magic, len(block.data))
    self.file.write(header + block.data)
    self.close()
      
class BlockWriter:
  def __init__(self):
    self.config = Config(configfile)
    self.receiver = BlockReceiver(self.config)
    self.receiver.start()
    while self.receiver.started == False:
      print("Trying to connect to rabbitmq...")
      self.receiver.start()
      time.sleep(10)
      
  def start(self):
    blockDirWriter = BlockDirWriter(self.config)
    while True:
      block = self.receiver.receiveBlock()
      if(block == None): 
        time.sleep(5)
        continue
      result = block.verify()
      if(result == False): continue
      blockDirWriter.writeBlock(block)
     
    blockDir.close()
    
  
BlockWriter().start()