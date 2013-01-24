from twisted.internet.protocol import DatagramProtocol
from collections import Counter, defaultdict

import config

class StatsdListener(DatagramProtocol):

    def __init__(self):
        self.type_handlers = {
            'ms': self.handleTimer,
            'c': self.handleCounter,
        }

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

