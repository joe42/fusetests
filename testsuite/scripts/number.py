#!/usr/bin/python
import sys
try:    
    float(sys.argv[1]); 
    exit(0); 
except ValueError: 
    exit(1); 
