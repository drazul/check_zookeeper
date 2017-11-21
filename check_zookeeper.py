#! /usr/bin/env python
#  Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
""" Check Zookeeper Cluster

Generic monitoring script that could be used with multiple platforms (Nagios).

It requires ZooKeeper 3.4.0 or greater. The script needs the 'mntr' 4letter word 
command (patch ZOOKEEPER-744) that was now commited to the trunk.
The script also works with ZooKeeper 3.3.x but in a limited way.
"""

from __future__ import print_function, unicode_literals
import sys
import socket
import logging
import re
import argparse

try:
    from io import StringIO
except:
    from StringIO import StringIO

__version__ = (0, 1, 0)

log = logging.getLogger()
logging.basicConfig(level=logging.ERROR)

class NagiosHandler(object):

    @classmethod
    def register_options(cls, parser):
        pass

    def is_critical(self, value, warning, critical):
        if critical is None:
            return False
        if warning is None:
            return value <= critical

        if critical > warning:
            return critical <= value
        else:
            return critical >= value
        
    def is_warning(self, value, warning, critical):
        if warning is None:
            return False
        if critical is None:
            return value <= warning

        if critical > warning:
            return warning <= value
        else:
            return warning >= value

    def _get_limits(self, args):
        limits = {}
        for k in args.key or []:
            w = None
            c = None
            n = k.count(',')
            if n == 2:
                name, w, c = k.split(',')
            elif n == 1:
                name, c = k.split(',')
            elif n > 2:
                print("Too many values for key. Format: 'name|name:c|name:w:c'. Ignored.", file=sys.stderr)
                continue
            else:
                name = k

            limits[name] = (int(w), int(c))
        return limits
        
    def analyze(self, args, cluster_stats):
        limits = self._get_limits(args)

        warning_state, critical_state, values = [], [], []
        for host, stats in cluster_stats.items():
            for stat in stats:
                warning, critical = limits.get(stat, (None, None))
                warning_str = warning if warning is not None else ''
                critical_str = critical if critical is not None else ''

                value = stats[stat]
                values.append('%s.%s=%s;%s;%s' % (host.replace(':', '_'), stat, value, warning_str, critical_str ))
    
                if self.is_critical(value, warning, critical):
                    critical_state.append("host: %s, key: %s, value: %s, warning: %s, critical: %s" % (host, stat, value, warning, critical))
                elif self.is_warning(value, warning, critical):
                    warning_state.append("host: %s, key: %s, value: %s, warning: %s, critical: %s" % (host, stat, value, warning, critical))

        output_str = ''
        output_status = 0
        if critical_state:
            output_str += 'CRITICAL\n'
            output_status = 2
        elif warning_state:
            output_str += 'WARNING\n'
            output_status = 1
        else:
            output_str += 'OK\n'
            output_status = 0
            
        output_str += 'critical\n' + '\n'.join(critical_state) + '\nwarning\n' + '\n'.join(warning_state) + '\n'
        output_str += '|\n' + '\n'.join(values)
        print(output_str)
        return output_status

class ZooKeeperServer(object):

    def __init__(self, host='localhost', port='2181', timeout=1):
        self._address = (host, int(port))
        self._timeout = timeout

    def get_stats(self):
        """ Get ZooKeeper server stats as a map """
        data = self._send_cmd('mntr')
        if data:
            return self._parse(data)
        else:
            data = self._send_cmd('stat')
            return self._parse_stat(data)

    def _create_socket(self):
        return socket.socket()

    def _send_cmd(self, cmd):
        """ Send a 4letter word command to the server """
        s = self._create_socket()
        s.settimeout(self._timeout)

        s.connect(self._address)
        s.send(cmd.encode())

        data = s.recv(2048)
        s.close()

        return data

    def _parse(self, data):
        """ Parse the output from the 'mntr' 4letter word command """
        h = StringIO(data.decode())
        
        result = {}
        for line in h.readlines():
            try:
                key, value = self._parse_line(line)
                result[key] = value
            except ValueError:
                pass # ignore broken lines

        return result

    def _parse_stat(self, data):
        """ Parse the output from the 'stat' 4letter word command """
        h = StringIO(data)

        result = {}
        
        version = h.readline()
        if version:
            result['zk_version'] = version[version.index(':')+1:].strip()

        # skip all lines until we find the empty one
        while h.readline().strip(): pass

        for line in h.readlines():
            m = re.match('Latency min/avg/max: (\d+)/(\d+)/(\d+)', line)
            if m is not None:
                result['zk_min_latency'] = int(m.group(1))
                result['zk_avg_latency'] = int(m.group(2))
                result['zk_max_latency'] = int(m.group(3))
                continue

            m = re.match('Received: (\d+)', line)
            if m is not None:
                result['zk_packets_received'] = int(m.group(1))
                continue

            m = re.match('Sent: (\d+)', line)
            if m is not None:
                result['zk_packets_sent'] = int(m.group(1))
                continue

            m = re.match('Outstanding: (\d+)', line)
            if m is not None:
                result['zk_outstanding_requests'] = int(m.group(1))
                continue

            m = re.match('Mode: (.*)', line)
            if m is not None:
                result['zk_server_state'] = m.group(1)
                continue

            m = re.match('Node count: (\d+)', line)
            if m is not None:
                result['zk_znode_count'] = int(m.group(1))
                continue

        return result 

    def _parse_line(self, line):
        try:
            key, value = (x.strip() for x in line.split('\t'))
        except ValueError:
            raise ValueError('Found invalid line: %s' % line)

        if not key:
            raise ValueError('The key is mandatory and should not be empty')

        try:
            value = int(value)
        except (TypeError, ValueError):
            pass

        return key, value

def main():
    args = parse_cli()

    cluster_stats = get_cluster_stats(args.servers)
    return NagiosHandler().analyze(args, cluster_stats)


def dump_stats(cluster_stats):
    """ Dump cluster statistics in an user friendly format """
    for server, stats in cluster_stats.items():
        print('Server: %s' % server)

        for key, value in stats.items():
            print("%30s %s" % (key, value))
        print

def get_cluster_stats(servers):
    """ Get stats for all the servers in the cluster """
    stats = {}

    for server in servers:
        host, port = server.split(':') if ':' in server else (server, '2181')
        try:
            zk = ZooKeeperServer(host, port)
            stats["%s:%s" % (host, port)] = zk.get_stats()

        except socket.error:
            # ignore because the cluster can still work even 
            # if some servers fail completely

            # this error should be also visible in a variable
            # exposed by the server in the statistics

            logging.info('unable to connect to server '\
                '"%s" on port "%s"' % (host, port))

    return stats


def get_version():
    return '.'.join(map(str, __version__))


def parse_cli():
    parser = argparse.ArgumentParser(description='Check for Zookeeper to Nagios')
    parser.add_argument('--version', action='version', version=get_version())

    parser.add_argument('-s', '--servers', nargs='+', 
        help='a list of SERVERS', metavar='SERVERS')

    parser.add_argument('-k', '--key', nargs='*',
        help=('List of commands to execute in format: command_name,warning,critical.'
              'Example --commands zk_avg_latency,10,50 zk_num_alive_connections,10,50\n'
              'List of available commands: zk_packets_sent, zk_outstanding_requests, zk_open_file_descriptor_count,'
              'zk_max_latency, zk_max_file_descriptor_count, zk_ephemerals_count, zk_packets_received, zk_min_latency,'
              'zk_approximate_data_size, zk_avg_latency, zk_znode_count, zk_watch_count, zk_num_alive_connections')
    )

    return parser.parse_args()


if __name__ == '__main__':
    sys.exit(main())

