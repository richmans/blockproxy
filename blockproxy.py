#!/usr/bin/env python
import time
import os
import struct
import pip
from blocklogic import *
import sys
try:
  import pika
except:
  pip.main(['install', "pika"])
  import pika

dirPath = os.path.dirname(os.path.realpath(__file__))
configfile = os.path.join(dirPath, 'blockproxy.json')


class RateLimiter:
  def __init__(self, config):
    self.maxRate = config.maxMbytePerSec * 1024 * 1024
    self.maxBlocks = config.maxBlocksPerSec
    self.rateCheckInterval = config.rateCheckInterval
    self.reset()

  def milliTime(self):
    return int(round(time.time() * 1000))
    
  def reset(self):
    self.lastRateReset = self.milliTime()
    self.bytesSinceLastReset = 0
    self.blocksSinceLastReset = 0
  
  def checkRate(self, lenBytes):
    currentTime = self.milliTime()
    if (currentTime - self.lastRateReset > self.rateCheckInterval):
      self.reset()
    maxBytes = self.maxRate * int(self.rateCheckInterval / 1000)
    maxBlocks = self.maxBlocks * int(self.rateCheckInterval / 1000)
    if self.bytesSinceLastReset > maxBytes or self.blocksSinceLastReset > maxBlocks:
      sleepTime = float((self.lastRateReset + self.rateCheckInterval) - currentTime) / 1000
      time.sleep(sleepTime)
    self.bytesSinceLastReset += lenBytes
    self.blocksSinceLastReset += 1
    
class BlockSender:
  def __init__(self, config):
    self.config = config
    self.started = False
    self.rateLimiter = RateLimiter(config)
    
  def start(self):
    try:
      credentials = pika.PlainCredentials(self.config.rabbitUser, self.config.rabbitPassword)
      self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                     self.config.rabbitHost,
                     5672,
                     self.config.rabbitVirtualHost,
                     credentials))
      self.channel = self.connection.channel()
      self.started = True
    except Exception as e:
      print("Rabbit connection error: " + str(type(e)))
  
  
  def publishBlock(self, block):
    self.rateLimiter.checkRate(len(block.data))
    key = struct.pack("<II",  block.fromFile, block.fileOffset)
    self.channel.basic_publish(exchange=self.config.rabbitExchange,
                          routing_key=self.config.rabbitRoutingKey,
                          body=key + block.data)
  def close(self):
    self.connection.close()
      
class BlockDir:
  def __init__(self, config, number, offset, maxNumber=None):
    self.number = number
    self.maxNumber = maxNumber
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
      self.file = None
  
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
    if self.maxNumber != None or (self.maxNumber != None and self.number <= self.maxNumber):
      self.open()
    
  def readBlock(self):
    if self.file == None: return None
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
    return Block(magic, length, data, self.number, self.offset)
    
class BlockProxy:
  def __init__(self, argv):
    self.config = Config(configfile)
    self.stopping = False
    self.singleFile = len(argv) > 1
    if self.singleFile:
      print("Single file mode")
      self.currentFile = int(argv[1])
      self.currentOffset = 0
      self.maxFile = self.currentFile
    else:
      self.currentFile = self.config.startFile
      self.currentOffset = self.config.startByte
      self.maxFile = None
    
    self.sender = BlockSender(self.config)
    while self.sender.started == False:
      self.sender.start()
      time.sleep(10)
    self.blocksSinceSave = 0
  
  def updateConfig(self):
    self.config.startFile = self.currentFile
    self.config.startByte = self.currentOffset
    self.config.save()
    
  def handleBlocks(self):
    blockDir = BlockDir(self.config, self.currentFile, self.currentOffset, self.maxFile)
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

BlockProxy(sys.argv).start()