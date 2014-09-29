#!/usr/bin/python
import os
import ConfigParser
import time
import shutil

HOME = os.environ['HOME']
nubisave_config_path = HOME+'/.nubisave/nubisavemount/config/config'
redundancy = 0

storages = 0
m = 0

new_storage = 999
previous_availability = -1

def get_config():
    config = ConfigParser.RawConfigParser()
    config.read(nubisave_config_path)
    return config

def set_redundancy(redundancy):
    config = get_config()
    config.set('splitter', 'redundancy', str(redundancy))
    #print "set redundancy to "+str(redundancy)
    with open(nubisave_config_path, 'wb') as configfile:
        config.write(configfile)

def availability_changed():
    global previous_availability
    config = get_config()
    availability = config.getfloat('splitter', 'availability')
    ret = availability != previous_availability
    previous_availability = availability
    return ret

def get_redundancy():
    config = get_config()
    return config.getint('splitter', 'redundancy')

def increase_availability():
    global redundancy
    #print "increase availability"
    while True:
        redundancy = get_redundancy()
        #config = get_config()
        #print "redundancy: %s av: %s"%(redundancy, config.getfloat('splitter', 'availability'))
        if availability_changed() or redundancy == 100:
            #print "availability increased: "+str(previous_availability)
            break
        set_redundancy(redundancy+1)
    
def add_new_storage_and_reset_redundancy():
    global new_storage
    global storages
    global m
    global redundancy
    new_storage += 1
    shutil.copy('../../splitter/mountscripts/directory.ini', HOME+'/.nubisave/nubisavemount/config/'+str(new_storage))
    set_redundancy(0)
    availability_changed() # reset availability
    redundancy = get_redundancy()
    storages += 1
    m = 0
    

def initialize_nubisave():
    config = get_config()
    config.set('splitter', 'storagestrategy', 'UseAllInParallel')
    with open(nubisave_config_path, 'wb') as configfile:
        config.write(configfile)
    add_new_storage_and_reset_redundancy()

def main():
    import os
    global storages
    global m
    switch = 0
    initialize_nubisave()
    while True:
        print "n: %s.%s m: %s r: %s  a:%s"%(storages, len(os.listdir(HOME+"/.nubisave/nubisavemount/config/"))-1, m, redundancy, previous_availability)#n
        os.system('../scripts/test_normal_files_simple.py %s/.nubisave/nubisavemount/data . "1 2 4 8 16 32 64 128 256 512 1024 1536 2048 3072 4096 6144 8192" KB' %HOME)
        increase_availability()
        m += 1
        time.sleep(1)
        if redundancy == 100:
            if switch == 0:
                switch = 1
                continue
            switch = 0
            add_new_storage_and_reset_redundancy()
            
if __name__ == '__main__':
    main()
