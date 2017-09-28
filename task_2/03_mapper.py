#!/usr/bin/env python3.6

import sys

################################################################################
# Main
################################################################################

for line in sys.stdin:
    print(line.split(',')[3][1:-2] + '\t' + line, end='')         # the line already contains EOL

