def average(l):
    if len(l) > 0:
        return sum(l)/len(l)
    else:
        return 0

functions = {'average': average, 'max': max, 'min': min, 'sum': sum}


class EventAccumulator:
    '''
    Accumulate a stream of values over powers of ten, i.e.,
    here's the {average,max,min} of the last 10 values, the last
    100 values, the last 1k, 10k, 100k, 1mm values.

    For laziness reasons, only the first 10 value is immediately
    updated; the others only change every 10th, 100th, etc.
    '''
    def __init__(self, levels=6, function='average'):
        self.levels = levels
        if function in functions:
            self.function = functions[function]
        else:
            raise ValueError('Invalid function name, valid ones are: ' + ''.join(functions.keys()))

        self.data = []
        for _ in range(0, self.levels):
            self.data.append([])

    def accumulate(self, value, debug=False):
        value = float(value)
        orig = value
        for l in range(0, self.levels):
            self.data[l].append(value)
            if len(self.data[l]) == 1:
                self.data[l].append(value)  # only at startup
            if len(self.data[l]) > 10:
                # this confuses pylint. all of the functions return floats
                value = self.function(self.data[l][1:])  # pylint: disable=redefined-variable-type
                self.data[l] = [value]
            else:
                break
        if debug:
            self.debug(orig)

    def debug(self, orig):
        print('Debug: data is now')
        for l in range(0, self.levels):
            print('level {}: {}'.format(l, ','.join(str(x) for x in self.data[l])))

    def read(self):
        ret = []
        last = 0.0
        if len(self.data[0]) > 1:  # immediate update for this one
            value = self.function(self.data[0][1:])
            self.data[0][0] = value
        for l in range(0, self.levels):
            if len(self.data[l]):
                last = self.data[l][0]
                ret.append(last)
            else:
                ret.append(last)
        return ret

# class TimeAccumulator:
#    '''
#    Accumulate event counts by time, over the past second, 10 seconds, minute,
#    10 minutes, hour, day, month. Kind of like an in-memory RRD.
#    '''
#    def __init__(self, levels=6, function='sum'):
