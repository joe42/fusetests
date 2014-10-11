#!/usr/bin/python
import sys, os
import argparse
import datetime
from time import sleep
import shutil
from sh import ErrorReturnCode
import time
from multiprocessing import Process
import tempfile
import collections
import traceback
try:
    from sh import dd
    from sh import git
    from sh import ifstat
except:
    traceback.print_exc()
    print 'Please install the python sh module first : pip install sh'
    exit(1)
from tabulate import tabulate
    
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

def wait_for_completed_upload(mountpoint, timeout_in_s = None):
    print "waiting for completed upload"
    if timeout_in_s is not None:
        print "waiting at most %s min" % (timeout_in_s/60)
    CLOUDFUSION_NOT_UPLOADED_PATH = mountpoint + "/stats/notuploaded"
    time_waited = 0
    if os.path.exists(CLOUDFUSION_NOT_UPLOADED_PATH):
        while os.path.getsize(CLOUDFUSION_NOT_UPLOADED_PATH) > 0:
            sleep(10)
            time_waited += 10
            if time_waited > timeout_in_s:
                break
        return
    
    start = time.time()
    
    def no_network_activity(line):
        try:
            kbit_per_5min = sum(map(int, line.split()))
            if kbit_per_5min < 200:
                return True
        except ValueError:
            pass
        if start + timeout_in_s < time.time():
            return True
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

class Ifstats(object):
    def __init__(self):
        self.tmp_stats = tempfile.NamedTemporaryFile()
        self._proc = ifstat('-tbzn', '1', _out=self.__ifstat_output)
    def __ifstat_output(self, line):
        self.tmp_stats.write(line)
    def stop(self):
        self._proc.kill()
    def store(self, filepath):
        shutil.copy(self.tmp_stats.name, filepath)
    def get_total_in_MB(self):
        (upload_in_KBps, download_in_KBps) = self.get_total_in_KB()
        upload_in_MBps = upload_in_KBps / 1000
        download_in_MBps = download_in_KBps / 1000
        return (upload_in_MBps, download_in_MBps)
    def get_total_in_KB(self):
        self.tmp_stats.seek(0)
        download_in_KBps = 0
        upload_in_KBps = 0
        for line in self.tmp_stats:
            try:
                _, up, down = line.split()
                download_in_KBps += float( up )
                upload_in_KBps += float( down )
            except ValueError: # Header of ifstat 
                pass
        return (upload_in_KBps, download_in_KBps)
    def get_average_transferrate_in_MBps(self):
        (upload_in_KBps, download_in_KBps) = self.get_average_transferrate_in_KBps()
        upload_in_MBps = upload_in_KBps / 1000
        download_in_MBps = download_in_KBps / 1000
        return (upload_in_MBps, download_in_MBps)
    def get_duration_in_s(self):
        self.tmp_stats.seek(0)
        total_time = 0
        for line in self.tmp_stats:
            try:
                _, _, _ = line.split()
                total_time += 1
            except ValueError: # Header of ifstat 
                pass
        return total_time
    def get_average_transferrate_in_KBps(self):
        up, down = self.get_total_in_KB()
        duration = self.get_duration_in_s() 
        return (up / duration, down / duration)
    def get_max_transferrate_in_MBps(self):
        (upload_in_KBps, download_in_KBps) = self.get_max_transferrate_in_KBps()
        upload_in_MBps = upload_in_KBps / 1000
        download_in_MBps = download_in_KBps / 1000
        return (upload_in_MBps, download_in_MBps)
    def get_max_transferrate_in_KBps(self):
        self.tmp_stats.seek(0)
        max_download_in_KBps = 0
        max_upload_in_KBps = 0
        for line in self.tmp_stats:
            try:
                _, up, down = line.split()
                download_in_KBps = float( up )
                upload_in_KBps = float( down )
                if upload_in_KBps > max_upload_in_KBps:
                    max_upload_in_KBps = upload_in_KBps
                if download_in_KBps > max_download_in_KBps:
                    max_download_in_KBps = download_in_KBps
            except ValueError: # Header of ifstat 
                pass
        return (max_upload_in_KBps, max_download_in_KBps)
    
def log_copy_operation(copy_source, copy_destination, file_size, nr_of_files, log_file, sample_files_dir, unit, mountpoint, check=False, timelimit_in_min=None):
    ''':returns: amount of files successfully written to the file system'''
    #check="$6" #check copy destination with the file of the name "file_sizeMB" in $SAMPLE_FILES_DIR
    success = 0
    errors = 0
    corruption = 0
    
    timelimit_in_s = timelimit_in_min * 60
    timeout = False

    LOG_DIR = os.path.dirname(log_file)+"/"+str(file_size)
    if check:
        LOG_DIR += "/read"
    else:
        LOG_DIR += "/write"
    os.makedirs(LOG_DIR)
      
    copy_stats_process = Process(target=periodic_copy_stats, args=(LOG_DIR, mountpoint))
    copy_stats_process.start()
    ifstats = Ifstats()
    time_before_operation = datetime.datetime.now() - datetime.timedelta(0)
    for nr in range(1, nr_of_files+1):  # from 1 to file quantity
        tries = 10
        while True:
            try:
                dd('if='+copy_source+str(nr), 'of='+copy_destination+str(nr), 'bs=131072')
                success += 1
            except ErrorReturnCode:
                tries -= 1
                errors += 1
                if tries == 0:
                    break 
                sys.stderr.write("Error occured during copying - retrying:")
                traceback.print_exc()
                continue
            break # stop loop if command succeeded
        timeout = timelimit_in_s < (datetime.datetime.now() - time_before_operation).seconds
        if timeout:
            break
    total_time_of_operation = (datetime.datetime.now() - time_before_operation).total_seconds()
    # Give upload process at least 10 minutes, to see how it behaves without load
    TEN_MIN = 60 * 10
    if timeout:
        max_wait_in_s = TEN_MIN
    else:
        max_wait_in_s = max( timelimit_in_s - total_time_of_operation, TEN_MIN)    
    wait_for_completed_upload(mountpoint, max_wait_in_s)
    timeout = timelimit_in_s < (datetime.datetime.now() - time_before_operation).seconds
    ifstats.stop()
    ifstats.store(LOG_DIR+'/ifstats_out')
    time.sleep(60)
    copy_stats_process.terminate()
    time_after_operations = datetime.datetime.now() - datetime.timedelta(0)
    time_of_multiple_operations = time_after_operations - time_before_operation
    print "time_before_operation %s - time_after_operation %s" % (time_before_operation, time_after_operations)
    for nr in range(1, nr_of_files+1):  # from 1 to file quantity
        if check:
            if not is_equal(copy_destination+str(nr), sample_files_dir+'/'+str(file_size)+unit+'_'+str(nr)):
                corruption += 1
    average_transfer_rate = (1.0*int(file_size) * success)*(1/time_of_multiple_operations.total_seconds())
    columns = collections.OrderedDict()
    columns['single file size'] = [file_size]
    if not timeout:
        columns['total size'] = [file_size * nr_of_files]
        columns['time for operation [s]'] = [time_of_multiple_operations.total_seconds()]
        columns['average transfer rate [%s/s]' % unit] = [average_transfer_rate]
    else:
        columns['total size'] = [ifstats.get_total_in_MB()]
        columns['time for operation [s]'] = [ifstats.get_duration_in_s()]
        columns['average transfer rate [MB/s]'] = [ifstats.get_average_transferrate_in_MBps()]
    columns['success'] = [success]
    columns['io errors'] = [errors]
    columns['corrupt files'] = [corruption]
        
    table = tabulate(columns, tablefmt="plain")
    if not os.path.exists(log_file):
        with open(log_file, 'w') as f:
            f.write(str(table))
    return success

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
        test_directory = data_directory+'/cftest3'
            
        log_directory = LOG_DIRECTORY+'/logs/streaming/'+os.path.basename(mountpoint)+'/'+now
        write_time_log = log_directory + '/write_time_log'
        read_time_log = log_directory + '/read_time_log'
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
            writen_files = log_copy_operation(SAMPLE_FILES_DIR+'/'+size+unit+'_', test_directory+'/'+size+unit+'_', size, filequantity_arr[idx], write_time_log, SAMPLE_FILES_DIR, unit, mountpoint, timelimit_in_min=60)
            print "Test reading file size %s %s"%(size,unit)
            log_copy_operation(test_directory+'/'+size+unit+'_', TEMP_DIR+'/'+size+unit+'_', size, writen_files, read_time_log, SAMPLE_FILES_DIR, unit, mountpoint, check=True, timelimit_in_min=60)
            for nr in reversed( range(1,filequantity_arr[idx]+1) ):  # from file quantity to 1
                os.remove(test_directory+'/'+size+unit+'_'+str(nr))
                os.remove(TEMP_DIR+'/'+size+unit+'_'+str(nr))
            idx += 1
        print "Created statistic files in "+log_directory
    
    #clean up before exit
    shutil.rmtree(TEMP_DIR)

if __name__ == '__main__':
    main()
