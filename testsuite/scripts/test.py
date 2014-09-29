#!/usr/bin/python
"""Generate directory of files for testing."""
import sys, os
import argparse
try:
    import sh
    from sh import dd
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
   print '          %s directory_for_generated_files' %  sys.argv[0]
   print '          Generate 10GB worth of 1MB, 2MB, 3MB, 4MB, 5MB, and 6MB files to the directory *directory_for_generated_files*.'
   print

def main():
    parser = MyParser()
    parser.add_argument('directory')
    args = parser.parse_args()
    directory = args.directory
    if not os.path.exists(directory):
        os.makedirs(directory)
    filesize_arr = [1,2,3,4,5,6]
    filequantity_arr = [10000,5000,3333,2500,2000,1667]
    
    idx = 0 
    for size in filesize_arr:
        for nr in range(1,filequantity_arr[idx]+1):  # from 1 to file quantity
            filename =  directory+'/'+str(size)+'MB_'+str(nr)
            print "writing "+directory+'/'+str(size)+'MB_'+str(nr)
            dd('if=/dev/zero', 'of='+filename, 'bs=1MB', 'count='+str(size))
        idx += 1
        
if __name__ == '__main__':
    main()

