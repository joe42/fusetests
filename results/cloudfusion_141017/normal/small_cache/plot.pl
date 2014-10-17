set xlabel "time [min]";
set xtic auto;
set ylabel "transfer rate [MB per s]";
set y2label "cached data [MB]";

set ytics nomirror
set y2tics nomirror 

set terminal pdf solid font "Helvetica,14" size 16cm,12cm
set output "upload_rates_vs_cache.pdf"
plot "table" using 1:2 title column(2)  w lines lt 1, "table" using 1:3 title column(3)  w lines lt 2 axes x1y2

