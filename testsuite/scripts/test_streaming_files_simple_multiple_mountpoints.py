#!/usr/bin/python
import sys, os
import argparse
import datetime
from time import sleep
import shutil
from sh import ErrorReturnCode
import time
from multiprocessing import Process
from sh import ifstat
try:
    from sh import dd
    from sh import git
except:
    import traceback
    traceback.print_exc()
    print 'Please install the python sh module first : pip install sh'
    exit(1)
    
# CONSTANTS
HOME = os.environ['HOME']
SAMPLE_FILES_DIR = '/tmp/cf/samplefiles'
now = datetime.datetime.today().strftime('%Y.%m.%d_%H.%M')
TEMP_DIR = '/tmp/cf/'+now 
    
class MyParser(argparse.ArgumentParser):
    def error(self, message):
        print_help()
        sys.exit(1)

def print_help():
    print
    print "Not enough arguments.\n"
    print 'Example: %s ~/mnt . "1 10 100 1000 2000" 4000 ' % sys.argv[0]
    print
    print 'Usage: %s mount_path log_directory test_file_sizes data_per_file_size [size_unit]' % sys.argv[0]
    print "    mount_path:         directory with multiple mount points"
    print "    log_directory:      a path to the directory to store log files"
    print "    test_file_sizes:    a list of numbers, which gives the size in MB of the test files, as well as their sequence "
    print "    data_per_file_size: determines how many files of each file size are written - i.e. choosing a value of 4000 could mean 4000 1MB files, 400 10 MB files, 2 2000MB, and 1 3000MB file. This should be bigger than the largest filesize."
    print '    size_unit:          the unit of the filesizes i.e. B, KB, MB, GB which are powers of 1000 Bytes (default is MB)'
    print
    print "Create directories log_directory/logs/streaming/MOUNT_POINT for each mountpoint in mount_path, where all logs are stored in a subdirectory with the name being the current date."
    print "Generate a number of files for each file size, so that all files of one size are about the size of specified through the data_per_file_size parameter."
    print "Log the time for copying each group of files with the same size to write_time_log. Directly after copying all of such a group of files, log the time to read these files from the storage service to read_time_log." 
    print "If the checksum of the read file differs from the original, this is noted as an unsuccessful operation."
    print "Log the time for reading the file from the storage service, which was copied in the step before, to previous_file_read_time_log and then delete this file. "
    print "The logs' format is as follows: "
    print "single_file_size    total_size    time_for_operation_in_seconds average_transfer_rate nr_of_files success"
    print "    single_file_size  - the size in MB of the files used for the operation"
    print "    total_size  - the total size in MB of all files in the same group"
    print "    time_for_operation_in_seconds  - how long it took to perform the operation on a group of files in seconds"
    print "    average_transfer_rate  - the averagetransfer rate in MB/s"
    print "    nr_of_files  - the number of files for this operation"
    print "    success  - number of successful copy operations"
    print
    

    


def is_equal(file1, file2):
    import hashlib 
    with open(file1) as f:
        data = f.read()    
    file1_md5 = hashlib.md5(data).hexdigest()
    with open(file2) as f:
        data = f.read()    
    file2_md5 = hashlib.md5(data).hexdigest()
    return file1_md5 == file2_md5

def wait_for_completed_upload(mountpoint):
    print "waiting for completed upload"
    CLOUDFUSION_NOT_UPLOADED_PATH = mountpoint + "/stats/notuploaded"
    if os.path.exists(CLOUDFUSION_NOT_UPLOADED_PATH):
        while os.path.getsize(CLOUDFUSION_NOT_UPLOADED_PATH) > 0:
            sleep(10)
        return

    def no_network_activity(line):
        try:
            kbit_per_5min = sum(map(int, x.split()))
            if kbit_per_5min < 200:
                return True
        except ValueError,e:
            pass
        return False
    p = ifstat('-bzn', '600', _out=(lambda x:no_network_activity ))
    p.wait()
    p.kill()
    
def periodic_copy_stats(log_directory, mountpoint):
    '''Copy cloudfusion's stats and errors files every 60 seconds
       to the log directory into either the subdirectory stats or errors.
       The files are renamed to the timestamp at the time they were copied.
       The subdirectories are created if they do not exist.'''
    CLOUDFUSION_STATS = mountpoint + "/stats/stats"
    if not os.path.exists(CLOUDFUSION_STATS):
        return
    CLOUDFUSION_ERRORS = mountpoint + "/stats/errors"
    STATS_LOG_DIR = log_directory + "/stats"
    ERRORS_LOG_DIR = log_directory + "/errors"
    os.makedirs(STATS_LOG_DIR)
    os.makedirs(ERRORS_LOG_DIR)
    while True:
        shutil.copy(CLOUDFUSION_STATS, STATS_LOG_DIR)
        now = datetime.datetime.today().strftime('%Y.%m.%d_%H.%M')
        shutil.move(STATS_LOG_DIR+"/stats", STATS_LOG_DIR+"/"+now)
        shutil.copy(CLOUDFUSION_ERRORS, ERRORS_LOG_DIR+"/errors")
        now = datetime.datetime.today().strftime('%Y.%m.%d_%H.%M')
        shutil.move(ERRORS_LOG_DIR+"/errors", ERRORS_LOG_DIR+"/"+now)
        time.sleep(60)
    
def log_copy_operation(copy_source, copy_destination, file_size, nr_of_files, log_file, sample_files_dir, unit, mountpoint, check=False):
    #check="$6" #check copy destination with the file of the name "file_sizeMB" in $SAMPLE_FILES_DIR
    success = 0

    LOG_DIR = os.path.dirname(log_file)+"/"+str(file_size)
    if check:
        LOG_DIR += "/read"
    else:
        LOG_DIR += "/write"
    os.makedirs(LOG_DIR)

    def ifstat_output(line):
        with open(LOG_DIR+'/ifstats_out', 'a') as file:
            file.write(line)

    copy_stats_process = Process(target=periodic_copy_stats, args=(LOG_DIR, mountpoint))
    copy_stats_process.start()
    ifstat_logger = ifstat('-tbzn', '1', _out=ifstat_output)
    time_before_operation = datetime.datetime.now() - datetime.timedelta(0)
    for nr in range(1, nr_of_files+1):  # from 1 to file quantity
        operation_succeeded=0
        while True:
            try:
                dd('if='+copy_source+str(nr), 'of='+copy_destination+str(nr), 'bs=131072')
            except ErrorReturnCode:
                import traceback
                sys.stderr.write("Error occured during copying - retrying:")
                traceback.print_exc()
                continue
            break # stop loop if command succeeded
        success += 1
    wait_for_completed_upload(mountpoint)
    ifstat_logger.kill()
    time.sleep(60)
    copy_stats_process.terminate()
    time_after_operations = datetime.datetime.now() - datetime.timedelta(0)
    time_of_multiple_operations = time_after_operations - time_before_operation
    print "time_before_operation %s - time_after_operation %s" % (time_before_operation, time_after_operations)
    for nr in range(1, nr_of_files+1):  # from 1 to file quantity
        if check:
            if is_equal(copy_destination+str(nr), sample_files_dir+'/'+str(file_size)+unit+'_'+str(nr)):
                success -= 1
    average_transfer_rate = (1.0*int(file_size) * nr_of_files)*(1/time_of_multiple_operations.total_seconds())
    #"single_file_size    total_size    time_for_operation_in_seconds average_transfer_rate nr_of_files success"
    if not os.path.exists(log_file):
        with open(log_file, 'w') as f:
            f.write("single file size\ttotal size\ttime for operation [s]\taverage transfer rate [%s/s]\tnumber of files\tsuccess\n" % unit)
    with open(log_file, 'a') as f:
        total_size = int(file_size)*nr_of_files
        f.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (file_size, total_size, time_of_multiple_operations.total_seconds(), average_transfer_rate, nr_of_files, success))

def get_immediate_subdirectories(a_dir):
    return [os.path.abspath(os.path.join(a_dir, name)) for name in os.walk(a_dir).next()[1]]



def main():
    parser = MyParser()
    parser.add_argument('mount_path')
    parser.add_argument('log_directory')
    parser.add_argument('filesize')
    parser.add_argument('filegroup_size')
    parser.add_argument('args', nargs=argparse.REMAINDER) #collect all arguments positioned after positional and optional parameters 
    args = parser.parse_args()
    if len(args.args) != 1:
        unit = 'MB'
    else:
        unit = args.args[0]
    MOUNT_DIRECTORY = args.mount_path
    MOUNT_DIRECTORIES = get_immediate_subdirectories(MOUNT_DIRECTORY)

    LOG_DIRECTORY = args.log_directory
    filesize_arr = args.filesize.split() 
    filegroup_size = int(args.filegroup_size)
    filequantity_arr =[]
    for size in map(int,filesize_arr):
        print "size: %s number of files in group: %s"%(size, int(round(1.0*filegroup_size/size)))
        filequantity_arr.append( int(round(1.0*filegroup_size/size)) )
    

    if not os.path.exists(SAMPLE_FILES_DIR):
        os.makedirs(SAMPLE_FILES_DIR)
    os.makedirs(TEMP_DIR)
    os.system(os.path.dirname(__file__)+"/create_streaming_files.py %s '%s' '%s' %s"%(SAMPLE_FILES_DIR, args.filesize, ' '.join(map(str,filequantity_arr)), unit))
    print "files created"    

    #iterate over all mountpoint directories
    for mountpoint in MOUNT_DIRECTORIES:
        data_directory = mountpoint+"/data"
        test_directory = data_directory+'/cftest'
        stats_directroy = mountpoint+"/stats"
        notuploaded_directroy = mountpoint+"/notuploaded"
        cloudfusion_config_path = mountpoint+"/config/config"
            
        log_directory = LOG_DIRECTORY+'/logs/streaming/'+os.path.basename(mountpoint)+'/'+now
        write_time_log = log_directory + '/write_time_log'
        read_time_log = log_directory + '/read_time_log'
        previous_file_read_time_log = log_directory + '/previous_file_read_time_log'
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
            
        print "starting tests"

        
        idx = 0 
        
        for size in filesize_arr:
            if os.path.exists('/tmp/stop'):
                print "waiting as long as /tmp/stop exists"
                while os.path.exists('/tmp/stop'):
                    sleep(1)
                print "continuing test"
            if not os.path.exists(test_directory):
                os.makedirs(test_directory)
            print "Test writing file size %s %s"%(size,unit)
            log_copy_operation(SAMPLE_FILES_DIR+'/'+size+unit+'_', test_directory+'/'+size+unit+'_', size, filequantity_arr[idx], write_time_log, SAMPLE_FILES_DIR, unit, mountpoint)
            print "Test reading file size %s %s"%(size,unit)
            log_copy_operation(test_directory+'/'+size+unit+'_', TEMP_DIR+'/'+size+unit+'_', size, filequantity_arr[idx], read_time_log, SAMPLE_FILES_DIR, unit, mountpoint, check=True)
            for nr in range(1,filequantity_arr[idx]+1):  # from 1 to file quantity
                os.remove(test_directory+'/'+size+unit+'_'+str(nr))
                os.remove(TEMP_DIR+'/'+size+unit+'_'+str(nr))
            idx += 1
        print "Created statistic files in "+log_directory
    
    #clean up before exit
    shutil.rmtree(TEMP_DIR)

if __name__ == '__main__':
    main()
