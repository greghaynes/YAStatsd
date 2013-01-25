from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor

from collections import Counter, defaultdict
import heapq

import config

class StatsdListener(DatagramProtocol):

    def __init__(self):
        self.type_handlers = {
            'ms': self.handleTimer,
            'c': self.handleCounter,
        }
        self.timers = defaultdict(list)
        self.timers_sum = 0
        self.timer_type_multipliers = {
            'ms': 1,
            's': 1000
        }
        self.counters = Counter()

    def datagramReceived(self, data, (host, port)):
        metrics = data.split('\n')
        for metric in metrics:
            self.metricReceived(metric, (host, port))

    def metricReceived(self, metric, (host, port)):
        met_split = metric.split('|')
        if len(met_split) == 2:
            event, event_type = met_split
            sampling = 1
        elif len(met_split) == 3:
            event, event_type, sampling = met_split
        else:
            raise ValueError('Ivalid number of "|"s, found %d'
                % len(met_split))
        event_name, event_str_val = event.split(':')
        event_val = int(event_str_val)
        self.type_handlers[event_type](event_name, event_type,
            event_val, sampling)

    def handleTimer(self, event_name, event_type, event_val, sampling):
        # Sampling is meaningless for a timer
        if sampling != 1:
            raise ValueError('Sampling specified for a timer type')
        
        event_val *= self.timer_type_multipliers[event_type]

        heappush(self.timers[event_name], event_val)
        self.timers_sum += event_val

    def handleCounter(self, event_name, event_type, event_val, sampling):
        self.counters[event_name] += event_val * sampling


def main():
    reactor.listenUDP(config.port, StatsdListener())
    reactor.run()

if __name__ == '__main__':
    main()
