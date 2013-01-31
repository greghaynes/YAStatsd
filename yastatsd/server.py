from twisted.internet.protocol import DatagramProtocol
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from collections import Counter, defaultdict, deque
import heapq
from time import time

import config

class StatsdServer(DatagramProtocol):

    def __init__(self):
        self.backends = []
        self.type_handlers = {
            'ms': self.handleTimer,
            'c': self.handleCounter,
            'g': self.handleGauge,
        }
        self.timers = defaultdict(list)
        self.timers_sum = Counter()
        self.counters = Counter()
        self.gauges_sum = Counter()
        self.gauges_count = Counter()

        # Setup flush routine
        self.flush_call = LoopingCall(self.handleFlush)
        self.flush_call.start(config.flushInterval)

    def addBackend(self, backend):
        self.backends.append(backend)

    def datagramReceived(self, data, (host, port)):
        metrics = data.split('\n')
        for metric in metrics:
            if metric != '':
                self.metricReceived(metric, (host, port))

    def metricReceived(self, metric, (host, port)):
        met_split = metric.split('|')
        if len(met_split) == 2:
            event, event_type = met_split
            sampling = '@1'
        elif len(met_split) == 3:
            event, event_type, sampling = met_split
        else:
            raise ValueError('Ivalid number of "|"s, found %d'
                % (len(met_split)+1))
        sampling = float(sampling[1:])
        event_name, event_str_val = event.split(':')
        event_val = int(event_str_val)
        self.type_handlers[event_type](event_name, event_type,
            event_val, sampling)

    def handleTimer(self, event_name, event_type, event_val, sampling):
        # Sampling is meaningless for a timer
        if sampling != 1:
            raise ValueError('Sampling specified for a timer type')
        
        heapq.heappush(self.timers[event_name], event_val)
        self.timers_sum[event_name] += event_val

    def handleCounter(self, event_name, event_type, event_val, sampling):
        if sampling != 1:
            self.counters[event_name] += event_val * (1 / sampling)
        else:
            self.counters[event_name] += event_val

    def handleGauge(self, event_name, event_type, event_val, sampling):
        self.gauges_sum[event_name] += event_val
        self.gauges_count[event_name] += 1

    def handleFlush(self):
        timeval = int(time())

        for backend in self.backends:
            backend.handleFlush(self, timeval)

        self.timers = defaultdict(list)
        self.timers_sum = Counter()
        self.gauges_sum = Counter()
        self.gauges_count = Counter()
        self.counters = Counter()


def run_server():
    ss = StatsdServer()

    from backends.graphite import GraphiteBackend
    gb = GraphiteBackend(config)
    ss.addBackend(gb)
    reactor.connectTCP(config.graphiteHost, config.graphitePort, gb)

    reactor.listenUDP(config.port, ss)
    reactor.run()

if __name__ == '__main__':
    run_server()
