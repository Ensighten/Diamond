# Copyright (C) 2011-2012 by Ivan Pouzyrevsky.
# Copyright (C) 2010-2011 by Brightcove Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from diamond import *
import diamond.collector
import diamond.convertor

_KEY_MAPPING = {
    'MemTotal'     ,
    'MemFree'      ,
    'Buffers'      ,
    'Cached'       ,
    'Active'       ,
    'Dirty'        ,
    'Inactive'     ,
    'SwapTotal'    ,
    'SwapFree'     ,
    'SwapCached'   ,
    'VmallocTotal' ,
    'VmallocUsed'  ,
    'VmallocChunk'
}

class MemoryCollector(diamond.collector.Collector):
    """
    This class collects data on memory utilization

    /proc/meminfo is used to gather the data, which is returned in units of kB
    """

    PROC = '/proc/meminfo'

    def collect(self):
        """
        Collect memory stats
        """
        if not os.access(self.PROC, os.R_OK):
            return None

        file = open(self.PROC)
        data = file.read()
        file.close()
        
        for line in data.splitlines():
            try:
                name, value, units = line.split()
                name = name.rstrip(':')
                value = int(value)
                
                if _KEY_MAPPING.has_key(name) and not self.config.has_key('detailed'):
                    continue
                
                if self.config.has_key('convert_to_bytes') :
                    if units == 'kB':
                        value *= 1024
    
                self.publish(name, value)
            except ValueError:
                continue