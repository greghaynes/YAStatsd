from twisted.internet.protocol import DatagramProtocol
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from collections import Counter, defaultdict, deque
import heapq
from time import time

import config

class StatsdServer(DatagramProtocol):

    def __init__(self, backends):
        self.backends = backends
        self.type_handlers = {
            'ms': self.handleTimer,
            'c': self.handleCounter,
        }
        self.timers = defaultdict(list)
        self.timers_sum = Counter()
        self.timer_type_multipliers = {
            'ms': 1,
            's': 1000
        }
        self.counters = Counter()

        # Setup flush routine
        self.flush_call = LoopingCall(self.handleFlush)
        self.flush_call.start(config.flushInterval)

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

        heapq.heappush(self.timers[event_name], event_val)
        self.timers_sum[event_name] += event_val

    def handleCounter(self, event_name, event_type, event_val, sampling):
        self.counters[event_name] += event_val * sampling

    def handleFlush(self):
        self.flushTimers()
        self.flushCounters()

    def flushTimers(self):
        flush_time = int(time())
        msgs = deque()
        timer_prefix = config.timerPrefix
        for timer_name, timer_vals in self.timers.items():
            for pct_threshold in config.percentThresholds:
                pct_ndx = int(float(pct_threshold) /
                    (len(timer_vals) * 100))
                pct_vals = heapq.nlargest(pct_ndx+1, timer_vals)
                pct_sum = sum(pct_vals)
                mean = pct_sum / float(len(pct_vals))
                msgs.extend((
                    '%s.%s.mean_%d %d %d\n' % (timer_prefix, timer_name,
                        pct_threshold, mean, flush_time),
                    '%s.%s.upper_%d %d %d\n' % (timer_prefix, timer_name,
                        pct_threshold, pct_vals[-1], flush_time),
                    '%s.%s.sum_%d %d %d\n' % (timer_prefix, timer_name,
                        pct_threshold, pct_sum, flush_time)))
            timer_sum = self.timers_sum[timer_name]
            mean = timer_sum / float(len(timer_vals))
            upper = heapq.nlargest(1, timer_vals)[0]
            msgs.extend((
                '%s.%s.mean %d %d\n' % (timer_prefix, timer_name, mean,
                    flush_time),
                '%s.%s.upper %d %d\n' % (timer_prefix, timer_name, upper,
                    flush_time),
                '%s.%s.sum %d %d\n' % (timer_prefix, timer_name, timer_sum,
                    flush_time)))
        self.timers = defaultdict(list)
        self.timers_sum = Counter()

    def flushCounters(self):
        flush_time = int(time())
        msgs = deque()
        counter_prefix = config.counterPrefix
        for counter_name, value in self.counters.items():
            msgs.append('%s.%s %d %d\n' (counter_prefix, counter_name,
                value, flush_time))
            if not self.deleteCounters:
                self.counters[counter_name] = 0
        if config.deleteCounters:
            self.counters = Counter()


def main():
    from backends.graphite import GraphiteBackend
    backends = (GraphiteBackend(), )
    reactor.listenUDP(config.port, StatsdServer(backends))
    reactor.run()

if __name__ == '__main__':
    main()
