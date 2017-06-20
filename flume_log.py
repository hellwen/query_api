
#-*- coding:utf-8 -*-
import logging
import logging.handlers

LOG_MAXBYTES = 2 * 1024 * 1024
LOG_BACKUPCOUNT = 5

# create logger
flume_log = logging.getLogger('flume')
flume_log.setLevel(logging.DEBUG)

# create formatter
# formatter = logging.Formatter('%(name)-12s: [line:%(lineno)d] %(levelname)-8s %(message)s')
formatter = logging.Formatter('%(asctime)s - %(lineno)s - %(name)s %(levelname)-8s %(message)s')

# create rotating file handler and set level to debug
LOG_FILE = "flume.log"
rf = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 5)
rf.setLevel(logging.DEBUG)
rf.setFormatter(formatter)

# create console handler and set level to error
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
#ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

# add handlers
flume_log.addHandler(ch)
#flume_log.addHandler(rf)

'''
1 Level:  CRITICAL  Numeric value: 50
2 Level:  ERROR     Numeric value: 40
3 Level:  WARNING   Numeric value: 30
4 Level:  INFO      Numeric value: 20
5 Level:  DEBUG     Numeric value: 10
6 Level:  NOTSET    Numeric value: 0
'''
