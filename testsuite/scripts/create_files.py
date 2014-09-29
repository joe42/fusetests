#!/usr/bin/python
"""Generate directory of random files for testing."""
import sys, os
import argparse
try:
    import sh
    from sh import openssl
except:
    print 'Please install the python sh module first : pip install sh'
    exit(1)

class MyParser(argparse.ArgumentParser):
    def error(self, message):
        print_help()
        sys.exit(1)

def print_help():
   print
   print '  Not enough arguments.\n'
   print '      Example: '
   print '          %s generated_files "1 10 100 1000 2000" MB' %  sys.argv[0]
   print '          Generate a 1MB file, a 10 MB file, a 100MB file, a 1000MB file, and a 2000MB file.'
   print
   print '      Usage: `basename $0` directory  test_file_sizes [size_unit]'
   print '          Creates a file of each size in the parameter test_file_sizes. ' 
   print '              directory:          the directory where the random files are stored to'
   print '              test_file_sizes:    the different filesizes of the generated files'
   print '              size_unit:          the unit of the filesizes i.e. B, KB, MB, GB which are powers of 1000 Bytes (default is MB)'            
   print '              The files are stored to the specified directory. All files in directory are deleted, when starting the script.'
   print

def main():
    parser = MyParser()
    parser.add_argument('directory')
    parser.add_argument('filesize')
    parser.add_argument('args', nargs=argparse.REMAINDER) #collect all arguments positioned after positional and optional parameters 
    args = parser.parse_args()

    if len(args.args) != 1:
        display_unit = 'MB'
        multiplicator = 1000*1000
    else:
        display_unit = args.args[0]
        if display_unit == 'B':
            multiplicator = 1
        elif display_unit == 'KB':
            multiplicator = 1000
        elif display_unit == 'MB':
            multiplicator = 1000*1000
        elif display_unit == 'GB':
            multiplicator = 1000*1000*1000
        elif display_unit == 'TB':
            multiplicator = 1000*1000*1000*1000
        else:
            display_unit = 'MB'
            multiplicator = 1000*1000
        
        
    directory = args.directory
    filesize_arr = args.filesize.split() #maps the list  ['1','2','3'] to [1,2,3]
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    idx = 0 
    for size in filesize_arr:
        filename = directory+'/'+size+display_unit
        if not os.path.exists(filename):
            print "writing "+directory+'/'+size+display_unit
            #faster random generator than /dev/urandom
            os.system("head /dev/zero -c%s | openssl enc -aes-256-ctr -pass pass:\"$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64)\" -nosalt > %s" % (multiplicator*int(size), filename))
        
if __name__ == '__main__':
    main()
