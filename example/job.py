__author__ = 'moxu'
from time import sleep
from datetime import datetime

def complete_calculate():
    print 'start calculating'
    curr_time = datetime.now()
    f = open(curr_time.strftime("%H-%M-%S"), 'a+')
    f.write('start calculating\n')
    sleep(10)
    f.write('finish!!')
    f.close()


def hello_world():
    print 'hello world'
    sleep(3)
    print 'finish'