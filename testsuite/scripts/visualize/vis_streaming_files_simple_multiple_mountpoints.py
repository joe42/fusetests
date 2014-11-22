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
                stats_dir = testdate_dir +"/"+filesize+"/write/stats"
                previous_upload = None
                lastdate_s = None
                start_time = None
                for stats_filename in sorted(os.listdir(stats_dir)):
                    stats_file = stats_dir+"/"+stats_filename
                    upload = _get_uploaded_mb(stats_file)
                    curdate_s = _get_time(stats_file)
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
                        # upload rate is smaller than zero if statistics are cleared
                        if upload_rate < 0:
                            upload_rate = 0
                        lastdate_s = curdate_s
                        previous_upload = upload
                        columns[header].append(upload_rate)
                        columns[date_header].append((curdate_s-start_time) / 60)
    _fill_empty_rows(columns)
    print "upload rates"
    print  tabulate(columns, tablefmt="plain")
    return tabulate(columns, tablefmt="plain")

def _get_time(stats_filepath):
    ''':returns: the time in seconds from the epoche, when *stats_filepath* was written'''
    curdate = datetime.strptime(os.path.basename(stats_filepath), "%Y.%m.%d_%H.%M") # parse data from filename
    return time.mktime(curdate.timetuple())

def _get_uploaded_mb(stats_filepath):
    line = sh.grep('MB uploaded', stats_filepath)
    num = line.split()[0]
    return float(num)

def _get_downloaded_mb(stats_filepath):
    line = sh.grep('MB downloaded', stats_filepath)
    num = line.split()[0]
    return float(num)

def _get_upload_rate_mb(stats_filepath):
    line = sh.grep('upload rate', stats_filepath)
    num = line.split()[0]
    return float(num)

def _get_cache_size_mb(stats_filepath):
    line = sh.grep('MB of cached data', stats_filepath)
    num = line.split()[0]
    return float(num)

def _fill_empty_rows(columns):
    '''*columns* represents a table with keys as column names 
    and values as a list of values for the column.
    The uneven columns are expected to represent the x-axis of a graph,
    while the even columns need to contain the y-axis values.
    All empty cells with x-axis values are filled with the value x:
    x is the value of the last non-empty row plus 1.
    All empty cells with y-axis values are filled with the value 0
    
    :param columns: dict -- represents a table '''
    max_rows = len(max(columns.values(), key=len))
    i = 0
    for k in columns.keys(): # x-axist
        i += 1
        if i %  2 == 0: # only uneven columns
            continue
        length = len(columns[k])
        diff = max_rows -length
        try:
            last_x_value = float( columns[k][-1] )
        except ValueError:
            last_x_value = 0
        columns[k].extend( [last_x_value+1]*diff )
    i = 0
    for k in columns.keys(): # y-axist
        i += 1
        if i %  2 == 1: # only even columns
            continue
        diff = max_rows - len(columns[k])
        columns[k].extend( [0]*diff )


def get_cache_size():
    columns = collections.OrderedDict()
    cnt_date_col = 1
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
                stats_dir = testdate_dir +"/"+filesize+"/write/stats"
                start_time = None
                for stats_filename in sorted(os.listdir(stats_dir)):
                    stats_file = stats_dir+"/"+stats_filename
                    cache_size = _get_cache_size_mb(stats_file)
                    curdate_s = _get_time(stats_file)
                    if start_time is None:
                        start_time = curdate_s
                    columns[header].append(cache_size)
                    columns[date_header].append((curdate_s-start_time) / 60)
    _fill_empty_rows(columns)
    print "cache size"
    print  tabulate(columns, tablefmt="plain")
    return tabulate(columns, tablefmt="plain")

def get_internal_average_upload_rate():
    columns = collections.OrderedDict()
    cnt_date_col = 1
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
                stats_dir = testdate_dir +"/"+filesize+"/write/stats"
                start_time = None
                for stats_filename in sorted(os.listdir(stats_dir)):
                    stats_file = stats_dir+"/"+stats_filename
                    upload_rate = _get_upload_rate_mb(stats_file)
                    curdate_s = _get_time(stats_file)
                    if start_time is None:
                        start_time = curdate_s
                    columns[header].append(upload_rate)
                    columns[date_header].append((curdate_s-start_time) / 60)
    _fill_empty_rows(columns)
    print "internal upload rates"
    print  tabulate(columns, tablefmt="plain")
    return tabulate(columns, tablefmt="plain")

def get_download_rates():
    columns = collections.OrderedDict()
    cnt_date_col = 1
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
                stats_dir = testdate_dir +"/"+filesize+"/read/stats"
                previous_download = None
                lastdate_s = None
                start_time = None
                for stats_filename in sorted(os.listdir(stats_dir)):
                    stats_file = stats_dir+"/"+stats_filename
                    download = _get_downloaded_mb(stats_file)
                    curdate_s = _get_time(stats_file)
                    if start_time is None:
                        start_time = curdate_s
                    if previous_download == None:
                        previous_download = download
                        continue
                    if lastdate_s is None:
                        lastdate_s = curdate_s
                    else:
                        diff_seconds = (curdate_s-lastdate_s)
                        download_rate = (download - previous_download) / diff_seconds
                        previous_download = download
                        columns[header].append(download_rate)
                        columns[date_header].append((curdate_s-start_time) / 60)
    _fill_empty_rows(columns)
    print "download rates"
    print  tabulate(columns, tablefmt="plain")
    return tabulate(columns, tablefmt="plain")

MONOCHROME = 'monochrome'

def plot(input_file, output_file, nr_of_lines, options = None, label = 'transfer rate [MB per s]'):
    '''Plot input file with uneven column number n being x axis value, 
    and n+1 being the corresponding y axis values for column n.'''
    if options is None:
        options = []
    with tempfile.NamedTemporaryFile() as plot_file:
        print >>plot_file, 'set xlabel "time [min]";'
        print >>plot_file, 'set xtic auto;'
        print >>plot_file, 'set ylabel "%s";' % label
        #print >>plot_file, 'set timefmt '%Y-%m-%d %H:%M:%S''
        if MONOCHROME in options:
            print >>plot_file, 'set terminal pdf monochrome solid font "Helvetica,14" size 16cm,12cm'
        else:
            print >>plot_file, 'set terminal pdf solid font "Helvetica,14" size 16cm,12cm'
        print >>plot_file, 'set output "%s"' % output_file 
        plot_file.write('plot ')
        print nr_of_lines
        for i in range(nr_of_lines):
            print "line:"+str(i)
            x_axis_col = i*2 + 1
            y_axis_col = i*2 + 2
            plot_file.write('"%s" using %s:%s title column(%s)  w lines ' % (input_file, x_axis_col, y_axis_col, y_axis_col))
            if i+1 != nr_of_lines:
                plot_file.write(',')
        plot_file.flush()
        print "plot file:"
        #print plot_file.name
        print sh.cat(plot_file.name)
        #raw_input("raw_input")
        sh.gnuplot(plot_file.name)


#set terminal pdf monochrome solid font 'Helvetica,14' size 16cm,12cm





def main():
    upload_rates = get_upload_rates()
    contains_data = len(upload_rates.splitlines()) > 2
    nr_of_lines = len(upload_rates.splitlines()[2].split()) / 2
    if contains_data: 
        with tempfile.NamedTemporaryFile() as data:
            print >>data, str(upload_rates)
            data.flush()
            plot(data.name, "upload_rates.pdf", nr_of_lines)
    cache_size = get_cache_size()
    contains_data = len(cache_size.splitlines()) > 2
    if contains_data: 
        with tempfile.NamedTemporaryFile() as data:
            print >>data, str(cache_size)
            data.flush()
            plot(data.name, "cache_size.pdf", nr_of_lines, label='cache size [MB]')
    upload_rates = get_internal_average_upload_rate()
    contains_data = len(upload_rates.splitlines()) > 2
    if contains_data: 
        with tempfile.NamedTemporaryFile() as data:
            print >>data, str(upload_rates)
            data.flush()
            plot(data.name, "internal_upload_rates.pdf", nr_of_lines)
    download_rates = get_download_rates()
    contains_data = len(download_rates.splitlines()) > 2
    if contains_data: 
        with tempfile.NamedTemporaryFile() as data:
            print >>data, str(download_rates)
            data.flush()
            plot(data.name, "download_rates.pdf", 1)

if __name__ == '__main__':
    main()

