from twisted.internet.task import LoopingCall

from collections import deque
import heapq

class GraphiteBackend(object):

    def __init__(self, config):
        self.config = config

    def handleFlush(self, stats, time):
        self.flushTimers(stats, time)
        self.flushCounters(stats, time)

    def flushTimers(self, stats, time):
        msgs = deque()
        timer_prefix = self.config.timerPrefix
        percent_thresholds = self.config.percentThresholds
        template_args = {
            'prefix': self.config.timerPrefix,
            'time': time
        }
        for timer_name, timer_vals in stats.timers.items():
            template_args['timer_name'] = timer_name
            for pct_threshold in percent_thresholds:
                template_args['pct'] = pct_threshold
                pct_ndx = int(float(pct_threshold) /
                    (len(timer_vals) * 100))
                pct_vals = heapq.nlargest(pct_ndx+1, timer_vals)
                pct_sum = sum(pct_vals)
                template_args['sum'] = pct_sum
                template_args['mean'] = pct_sum / float(len(pct_vals))
                template_args['upper'] = pct_vals[-1]
                msgs.extend((
                    '%(prefix)s.%(timer_name)s.mean_%(pct)d %(mean)d'\
                        ' %(time)d\n' % template_args,
                    '%(prefix)s.%(timer_name)s.upper_%(pct)d %(upper)d'\
                        ' %(time)d\n' % template_args,
                    '%(prefix)s.%(timer_name)s.sum_%(pct)d %(sum)d '\
                        '%(time)d\n' % template_args))
            timer_sum = stats.timers_sum[timer_name]
            template_args['sum'] = timer_sum
            template_args['mean'] = timer_sum / float(len(timer_vals))
            template_args['upper'] = heapq.nlargest(1, timer_vals)[0]
            msgs.extend((
                '%(prefix)s.%(timer_name)s.mean %(mean)d %(time)d\n'
                    % template_args,
                '%(prefix)s.%(timer_name)s.upper %(upper)d %(time)d\n'
                    % template_args,
                '%(prefix)s.%(timer_name)s.sum %(sum)d %(time)d\n'
                    % template_args))

    def flushCounters(self, stats, time):
        msgs = deque()
        for name, value in stats.counters.items():
            msgs.append('%(prefix)s.%(name)s %(value)f %(time)d' %
                { 
                    'prefix': self.config.counterPrefix,
                    'name': name,
                    'value': value,
                    'time': time
                });
