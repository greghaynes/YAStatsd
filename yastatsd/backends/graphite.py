from twisted.internet.protocol import ClientFactory, Protocol
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from collections import deque
import heapq
import math


class GraphiteConnection(Protocol):

    def __init__(self, factory):
        self.factory = factory

    def connectionMade(self):
        self.factory.connections.append(self)

    def connectionLost(self, reason):
        self.factory.connections.remove(self)


class GraphiteBackend(ClientFactory):

    def __init__(self, config):
        self.config = config
        self.connections = []

    def buildProtocol(self, addr):
        return GraphiteConnection(self)

    def clientConnectionLost(self, connector, reason):
        print 'Connection to graphite lost, Reason:', reason
        conn_retry = int(self.config.get("Graphite", "ConnRetryInterval"))
        print 'Retrying in %d seconds' % conn_retry
        reactor.callLater(conn_retry, connector.connect)

    def clientConnectionFailed(self, connector, reason):
        print 'Connection to graphite failed, Reason:', reason
        conn_retry = int(self.config.get("Graphite", "ConnRetryInterval"))
        print 'Retrying in %d seconds' % conn_retry
        reactor.callLater(conn_retry, connector.connect)

    def handleFlush(self, stats, time):
        self.flushTimers(stats, time)
        self.flushCounters(stats, time)
        self.flushGauges(stats, time)

    def flushTimers(self, stats, time):
        msgs = deque()
        timer_prefix = self.config.get("Graphite", "TimerPrefix")
        percent_thresholds = [int(x) for x in
            self.config.get("YAStatsd", "PercentThresholds").split(',')]
        template_args = {
            'prefix': timer_prefix,
            'time': time
        }
        pct_template = '%(prefix)s.%(timer_name)s.%(stat_type)s_%(pct)d %(val)f\n'
        val_template = '%(prefix)s.%(timer_name)s.%(stat_type)s %(val)f %(time)d\n'
        stat_types = ('sum', 'mean', 'upper')
        for timer_name, timer_vals in stats.timers.items():
            template_args['timer_name'] = timer_name

            for pct_threshold in percent_thresholds:
                template_args['pct'] = pct_threshold
                pct_elem_cnt = int(math.ceil(len(timer_vals) * pct_threshold * .01))
                pct_vals = heapq.nsmallest(pct_elem_cnt, timer_vals)
                pct_sum = sum(pct_vals)
                template_args['sum'] = pct_sum
                template_args['mean'] = pct_sum / float(len(pct_vals))
                template_args['upper'] = pct_vals[-1]

                for stat_type in stat_types:
                    msgs.append(pct_template % dict(
                            template_args,
                            stat_type=stat_type,
                            val=template_args[stat_type]))

            timer_sum = stats.timers_sum[timer_name]
            template_args['sum'] = timer_sum
            template_args['mean'] = timer_sum / float(len(timer_vals))
            template_args['upper'] = heapq.nlargest(1, timer_vals)[0]

            for stat_type in stat_types:
                msgs.append(val_template % dict(
                    template_args,
                    stat_type=stat_type,
                    val=template_args[stat_type]))

        for conn in self.connections:
            for msg in msgs:
                conn.transport.write(msg)
            conn.transport.write('%(prefix)s.%(name)s %(val)d %(time)d\n' %
                {
                    'prefix': self.config.get("Graphite", "YAStatsdPrefix"),
                    'name': 'timers.count',
                    'val': len(stats.timers),
                    'time': time
                })

    def flushCounters(self, stats, time):
        msgs = deque()
        for name, value in stats.counters.items():
            msgs.append('%(prefix)s.%(name)s %(value)f %(time)d\n' %
                { 
                    'prefix': self.config.get("Graphite", "CounterPrefix"),
                    'name': name,
                    'value': value / float(self.config.get("YAStatsd", "FlushInterval")),
                    'time': time
                })
        for conn in self.connections:
            for msg in msgs:
                conn.transport.write(msg)
            conn.transport.write('%(prefix)s.%(name)s %(val)d %(time)d\n' %
                {
                    'prefix': self.config.get("Graphite", "YAStatsdPrefix"),
                    'name': 'counters.count',
                    'val': len(stats.counters),
                    'time': time
                })

    def flushGauges(self, stats, time):
        msgs = deque()
        for name, value in stats.gauges_sum.items():
            avg = value / float(stats.gauges_count[name])
            msgs.append('%(prefix)s.%(name)s %(avg)f %(time)d\n' %
                {
                    'prefix': self.config.get("Graphite", "GaugePrefix"),
                    'name': name,
                    'avg': avg,
                    'time': time
                })
        for conn in self.connections:
            for msg in msgs:
                conn.transport.write(msg)
            conn.transport.write('%(prefix)s.%(name)s %(val)d %(time)d\n' %
                {
                    'prefix': self.config.get("Graphite", "YAStatsdPrefix"),
                    'name': 'gauges.count',
                    'val': len(stats.gauges_count),
                    'time': time
                })
