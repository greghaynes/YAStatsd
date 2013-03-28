from twisted.internet.protocol import DatagramProtocol
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from collections import Counter, defaultdict, deque
import heapq
from time import time
import argparse
import ConfigParser

class StatsdServer(DatagramProtocol):

    def __init__(self, config):
        self.config = config
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
        self.parseRepeaters()

        # Setup flush routine
        self.flush_call = LoopingCall(self.handleFlush)
        self.flush_call.start(float(config.get("YAStatsd", "FlushInterval")))

    def addBackend(self, backend):
        self.backends.append(backend)

    def datagramReceived(self, data, (host, port)):
        self.repeat(data)
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

    def parseRepeaters(self):
        self.repeaters = []
        if (self.config.has_option("YAStatsd", "repeater") and
                len(self.config.get("YAStatsd", "repeater")) > 0):
            for rep in self.config.get("YAStatsd", "repeater").split(','):
                if ":" in rep:
                    addr = rep.split(":")
                    self.repeaters.append(
                        (addr[0].strip(), int(addr[1].strip())))
                else:
                    self.repeaters.append(
                        (rep.strip(),
                         int(self.config.get("YAStatsd", "Port"))))

    def repeat(self, data):
        for rep in self.repeaters:
            self.transport.write(data, rep)


def run_server(config):
    ss = StatsdServer(config)

    from backends.graphite import GraphiteBackend
    gb = GraphiteBackend(config)
    ss.addBackend(gb)
    reactor.connectTCP(
        config.get("Graphite", "Host"),
        int(config.get("Graphite", "Port")),
        gb)

    reactor.listenUDP(int(config.get("YAStatsd", "Port")), ss)
    reactor.run()


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("config", type=str,
        help="Path to configuration file")
    args = argparser.parse_args()

    config = ConfigParser.SafeConfigParser()
    config.read(args.config)
    run_server(config)

if __name__ == '__main__':
    main()
