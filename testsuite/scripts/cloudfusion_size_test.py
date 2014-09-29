#!/usr/bin/python
import os
import ConfigParser
import time
import shutil
import os  

HOME = os.environ['HOME']
cloudfusion_data_path = HOME+'/mnt/data'
configfile = HOME+'/Sugarsync.ini'
cloudfusion_config_path = HOME+'/mnt/config/config'
cloudfusion_notuploaded_path = HOME+'/mnt/stats/notuploaded'
cloudfusion_stats_path = HOME+'/mnt/stats'

def restart_cloudfusion():
    os.system("pkill -9 -f  \"./env/bin/python.*\";fusermount -zu "+HOME+"/mnt;rm /tmp/cloudfusion/cachingstore* -r;rm .cloudfusion/logs/*;sleep 10")
    os.system("cloudfusion --config "+configfile+" "+HOME+"/mnt")

def main():
    size_in_bytes = 1
    while size_in_bytes <= 1000000:
        restart_cloudfusion()
        os.system('../scripts/test_streaming_files_cloudfusion.py %s . "'+size_in_bytes+'" 1000000 KB' % (cloudfusion_data_path))
        size_in_bytes *= 10
            
if __name__ == '__main__':
    main()
    
