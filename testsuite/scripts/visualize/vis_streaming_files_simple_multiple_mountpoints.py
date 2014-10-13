import os, sh
from tabulate import tabulate
from datetime import datetime
import time
import tempfile
import collections
import ipdb

def _get_dirs(dir_path):
    ''':returns: list of directories in *dir_path*'''
    return filter((lambda x:os.path.isdir(dir_path+"/"+x)), os.listdir(dir_path))

dir = _get_dirs(".")

def get_upload_rates():
    columns = collections.OrderedDict()
    cnt_date_col = 1
    len_lastcol = 0
    for service in dir:
        for testdate in _get_dirs(service):
            testdate_dir = service+"/"+testdate
            for filesize in filter((lambda x:x.isdigit()), _get_dirs(testdate_dir)):
                date_header = 'Date'+str(cnt_date_col)
                cnt_date_col += 1
                columns[date_header] = [date_header] 
                header = "\"%s\"" % repr((service, filesize))
                print "add new header for filesize: "+header
                columns[header] = [header]
                columns[header].extend([0]*len_lastcol)
                stats_dir = testdate_dir +"/"+filesize+"/write/stats"
                previous_upload = None
                lastdate_s = None
                start_time = None
                for stats_filename in sorted(os.listdir(stats_dir)):
                    stats_file = stats_dir+"/"+stats_filename
                    line = sh.grep('MB uploaded', stats_file)
                    num = line.split()[0]
                    upload = float(num)
                    curdate = datetime.strptime(stats_filename, "%Y.%m.%d_%H.%M") # parse data from filename
                    curdate_s = time.mktime(curdate.timetuple())
                    if start_time is None:
                        start_time = curdate_s
                    if previous_upload == None:
                        previous_upload = upload
                        continue
                    if lastdate_s is None:
                        lastdate_s = curdate_s
                    else:
                        diff_seconds = (curdate_s-lastdate_s)
                        upload_rate = (upload - previous_upload) / diff_seconds
                        previous_upload = upload
                        columns[header].append(upload_rate)
                        columns[date_header].append((curdate_s-start_time) / 60)
    max_rows = len(max(columns.values(), key=len))
    i = 0
    for k in columns.keys():
        i += 1
        if i %  2 == 0: # only uneven columns
            continue
        print "datecol:"+str(i)
        length = len(columns[k])
        diff = max_rows -length
        last_x_value = columns[k][-1]
        print "last val:"+str(last_x_value+1)
        columns[k].extend( [last_x_value+1]*diff )
        print columns[k]
    i = 0
    for k in columns.keys():
        i += 1
        if i %  2 == 1: # only even columns
            continue
        print "datacol:"+str(i)
        diff = max_rows - len(columns[k])
        columns[k].extend( [0]*diff )
        print columns[k]
    print  tabulate(columns, tablefmt="plain")
    return tabulate(columns, tablefmt="plain")



def get_internal_average_upload_rate():
    columns = {}
    cnt_date_col = 1
    len_lastcol = 0
    for service in dir:
        for testdate in _get_dirs(service):
            testdate_dir = service+"/"+testdate
            for filesize in filter((lambda x:x.isdigit()), _get_dirs(testdate_dir)):
                date_header = 'Date'+str(cnt_date_col)
                cnt_date_col += 1
                columns[date_header] = [date_header] 
                header = "\"%s\"" % repr((service, filesize))
                print "add new header for filesize: "+header
                columns[header] = [header]
                columns[header].extend([0]*len_lastcol)
                stats_dir = testdate_dir +"/"+filesize+"/write/stats"
                start_time = None
                for stats_filename in sorted(os.listdir(stats_dir)):
                    stats_file = stats_dir+"/"+stats_filename
                    line = sh.grep('upload rate', stats_file)
                    num = line.split()[0]
                    upload_rate = float(num)
                    curdate = datetime.strptime(stats_filename, "%Y.%m.%d_%H.%M") # parse data from filename
                    curdate_s = time.mktime(curdate.timetuple())
                    if start_time is None:
                        start_time = curdate_s
                    columns[header].append(upload_rate)
                    columns[date_header].append((curdate_s-start_time) / 60)
                        
    return tabulate(columns, tablefmt="plain")


def get_download_rates():
    columns = {}
    for service in dir:
        for testdate in _get_dirs(service):
            testdate_dir = service+"/"+testdate
            for filesize in filter((lambda x:x.isdigit()), _get_dirs(testdate_dir)):
                header = "\"%s\"" % repr((service, filesize))
                columns[header] = [header]
                stats_dir = testdate_dir +"/"+filesize+"/read/stats"
                previous_upload = None
                for stats_filename in sorted(os.listdir(stats_dir)):
                    stats_file = stats_dir+"/"+stats_filename
                    line = sh.grep('MB downloaded', stats_file)
                    num = line.split()[0]
                    upload = float(num)
                    if previous_upload == None:
                        previous_upload = upload
                        continue
                    upload_rate = (upload - previous_upload) / 60.0
                    previous_upload = upload
                    columns[header].append(upload_rate)
                    print (service,upload)
    return tabulate(columns, tablefmt="plain")

MONOCHROME = 'monochrome'

def plot(input_file, output_file, nr_of_lines, options = None):
    '''Plot input file with uneven column number n being x axis value, 
    and n+1 being the corresponding y axis values for column n.'''
    if options is None:
        options = []
    with tempfile.NamedTemporaryFile() as plot_file:
        print >>plot_file, 'set xlabel "time [min]";'
        print >>plot_file, 'set xtic auto;'
        print >>plot_file, 'set ylabel "transfer rate [MB per s]";'
        #print >>plot_file, 'set timefmt '%Y-%m-%d %H:%M:%S''
        if MONOCHROME in options:
            print >>plot_file, 'set terminal pdf monochrome solid font "Helvetica,14" size 16cm,12cm'
        else:
            print >>plot_file, 'set terminal pdf solid font "Helvetica,14" size 16cm,12cm'
        print >>plot_file, 'set output "%s"' % output_file 
        plot_file.write('plot ')
        for i in range(nr_of_lines):
            x_axis_col = i*2 + 1
            y_axis_col = i*2 + 2
            plot_file.write('"%s" using %s:%s title column(%s)  w lines ' % (input_file, x_axis_col, y_axis_col, y_axis_col))
            if i+1 != nr_of_lines:
                plot_file.write(',')
        plot_file.flush()
        print plot_file.name
        print input_file
        
        raw_input("stpping")
        sh.gnuplot(plot_file.name)


#set terminal pdf monochrome solid font 'Helvetica,14' size 16cm,12cm





def main():
    upload_rates = get_upload_rates()
    with tempfile.NamedTemporaryFile() as data:
        print >>data, str(upload_rates)
        data.flush()
        plot(data.name, "upload_rates.pdf", 1)

if __name__ == '__main__':
    main()
