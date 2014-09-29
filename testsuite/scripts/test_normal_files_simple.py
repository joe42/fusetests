#!/usr/bin/python
import sys, os
import argparse
import datetime
from time import sleep
import shutil
from sh import ErrorReturnCode
try:
    from sh import dd
    from sh import git
except:
    import traceback
    traceback.print_exc()
    print 'Please install the python sh module first : pip install sh'
    exit(1)
    
class MyParser(argparse.ArgumentParser):
    def error(self, message):
        print_help()
        sys.exit(1)

def print_help():
    print
    print "Not enough arguments.\n"
    print 'Example: %s ~/data . "1 10 100 1000 2000" 4000 ' % sys.argv[0]
    print
    print 'Usage: %s storage_path log_directory test_file_sizes data_per_file_size [size_unit]' % sys.argv[0]
    print "    storage_path:       a path to the directory to test"
    print "    log_directory:      a path to the directory to store log files"
    print "    test_file_sizes:    a list of numbers, which gives the size in MB of the test files"
    print '    size_unit:          the unit of the filesizes i.e. B, KB, MB, GB which are powers of 1000 Bytes (default is MB)'
    print
    print "Simple version of test_normal_files.sh. It does not clear the cache of the tested file system before starting to read from it."
    print "It does not ever start or stop the storage service."
    print "It does not log memory, network, or cpu load."
    print "It does not wait until the network transfer is over, but assumes the operation is over as soon as the file operation is done."
    print "Create a directory log_directory/logs/normal, where all logs are stored in a subdirectory with the name being the current date."
    print "Creates a file diff in the log directory to keep track of differences to the current revision."
    print "Log the time for copying each group of files with the same size to write_time_log. Directly after copying all of such a group of files, log the time to read these files from the storage service to read_time_log." 
    print "If the checksum of the read file differs from the original, this is noted as an unsuccessful operation."
    print "Log the time for reading the file from the storage service, which was copied in the step before, to previous_file_read_time_log and then delete this file. "
    print "The logs' format is as follows: "
    print "file_size    time_for_operation_in_seconds    success"
    print "    file_size  - the size in MB of the file used for the operation"
    print "    time_for_operation_in_seconds  - how long it took to perform the operation in seconds"
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

def log_copy_operation(copy_source, copy_destination, file_size, log_file, sample_files_dir, unit, check=False):
    #check="$6" #check copy destination with the file of the name "file_sizeMB" in $SAMPLE_FILES_DIR
    now = datetime.datetime.now()
    success = 0
    while True:
        try:
            start = datetime.datetime.now()
            dd('if='+copy_source, 'of='+copy_destination, 'bs=131072')
            
            end = datetime.datetime.now()
        except ErrorReturnCode:
            
            import traceback
            sys.stderr.write("Error occured during copying - retrying:")
            traceback.print_exc()
            continue
        break # stop loop if command succeeded
    time_of_operation = end - start 
    success += 1
    if check:
        if is_equal(copy_destination, sample_files_dir+'/'+str(file_size)+unit):
            success -= 1
    transfer_rate = (1.0*int(file_size) )*(1/time_of_operation.total_seconds())
    #"file_size    time_for_operation_in_seconds transfer_rate    success"
    if not os.path.exists(log_file):
        with open(log_file, 'w') as f:
            f.write("file size\ttime for operation [s]\ttransfer rate [%s/s]\tsuccess\n" % unit)
    with open(log_file, 'a') as f:
        f.write("%s\t%s\t%s\t%s\n" % (file_size, time_of_operation.total_seconds(), transfer_rate, success))


def main():
    parser = MyParser()
    parser.add_argument('storage_path')
    parser.add_argument('log_directory')
    parser.add_argument('filesize')
    parser.add_argument('args', nargs=argparse.REMAINDER) #collect all arguments positioned after positional and optional parameters 
    args = parser.parse_args()
    if len(args.args) != 1:
        unit = 'MB'
    else:
        unit = args.args[0]
    storage_path = args.storage_path
    log_directory = args.log_directory
    now = datetime.datetime.today().strftime('%Y.%m.%d_%H.%M.%S.%f')
    log_directory = log_directory+'/logs/normal/'+now
    filesize_arr = args.filesize.split() 
        
    sample_files_dir = '/tmp/samplefiles'
    temp_dir = '/tmp/'+now 
    write_time_log = log_directory + '/write_time_log'
    read_time_log = log_directory + '/read_time_log'
    previous_file_read_time_log = log_directory + '/previous_file_read_time_log'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    if not os.path.exists(storage_path):
        os.makedirs(storage_path)
    os.makedirs(temp_dir)
    with open(log_directory+'/diff', 'w') as f:
        f.write(git('--no-pager', 'diff', '--no-color').stdout)
        
    if os.path.exists(storage_path+'/../config/config'):
        shutil.copyfile(storage_path+'/../config/config', log_directory+'/config')
    print "starting tests"

    os.system(os.path.dirname(__file__)+"/create_files.py %s '%s' %s"%(sample_files_dir, args.filesize, unit))
    print "files created"
    for size in filesize_arr:
        if os.path.exists('/tmp/stop'):
            print "waiting as long as /tmp/stop exists"
            while os.path.exists('/tmp/stop'):
                sleep(1)
            print "continuing test"
        print "Test writing file size %s %s"%(size,unit)
        log_copy_operation(sample_files_dir+'/'+size+unit, storage_path+'/'+size+unit, size, write_time_log, sample_files_dir, unit)
        print "Test reading file size %s %s"%(size,unit)
        log_copy_operation(storage_path+'/'+size+unit, temp_dir+'/'+size+unit, size, read_time_log, sample_files_dir, unit, check=True)
        os.remove(storage_path+'/'+size+unit)
        os.remove(temp_dir+'/'+size+unit)
    shutil.rmtree(temp_dir)
    print "Created statistic files in "+log_directory

if __name__ == '__main__':
    main()
    
    
    
