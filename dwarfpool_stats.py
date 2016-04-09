"""
Script used to parse and save the table 'Shares for last 24 hours' from dwarfpool.com
"""

import csv
import os
import sys
import time
from BeautifulSoup import BeautifulSoup

import requests

from daemon import Daemon

STATS_URL = 'http://dwarfpool.com/eth/address'
payload = {'wallet': ''}

# full path is needed
OUTPUT_FILE = 'output.csv'

SLEEP_DURATION = 1800


def get_last_line(fin):
    line_len = 80
    fin.seek(0, os.SEEK_END)
    file_size = fin.tell()
    while True:
        line_len = min(line_len * 2, file_size)
        fin.seek(-line_len, os.SEEK_END)
        lines = fin.readlines()
        if len(lines) > 1 or line_len == file_size:
            return lines[-1]


class DwarfpoolDaemon(Daemon):
    def run(self):

        print 'Running the daemon'
        # main loop
        while True:

            r = requests.get(STATS_URL, params=payload)
            soup = BeautifulSoup(r.text)

            raw_data = []
            i = 0
            for table in soup.findAll('tbody'):
                if i == 2:
                    for row in table.findAll('tr'):
                        cols = [ele.text.strip() for ele in row]
                        raw_data.append(cols)
                i += 1

            # process the data
            data = []
            for ii in raw_data:
                # skip the rows where eth amount is still not exactly known
                if 'precalculated' in ii[3]:
                    continue

                # timestamp from date (assumes local tz)
                t = time.mktime(time.strptime(ii[0][:18], '%y-%m-%d, %H:%M:%S'))
                # shares
                s = int(ii[1])
                # percent of round
                p = float(ii[2])
                # amount ETH (assume its float)
                a = float(ii[3])

                data.append([t, s, p, a])

            # get the time of the latest entry
            t_last = 0
            print os.getcwd()
            if os.path.exists(OUTPUT_FILE):
                with open(OUTPUT_FILE, 'r') as f:
                    t_last = float(get_last_line(f).split(',')[0])

            # determine if there is new data
            if len(data) > 0 and t_last < data[0][0]:
                # save new data in reversed order
                with open(OUTPUT_FILE, 'a+') as f:
                    writer = csv.writer(f)
                    for irow in reversed(data):
                        if t_last < irow[0]:
                            writer.writerow(irow)

            time.sleep(SLEEP_DURATION)


if __name__ == "__main__":

    if payload['wallet'] == '':
        print 'Wallet address is empty'
        sys.exit(2)

    daemon = DwarfpoolDaemon('/tmp/daemon-example.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
