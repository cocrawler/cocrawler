class TimeBin:
    '''
    Given a series of values over time intervals, rebin them into N-second bins
    Useful for reporting 1-second and 30-second values to Carbon when your process
    is busy and sometimes produces values late
    '''

    def __init__(self, interval=1.0):
        self.interval = interval
        self.t0 = 0.
        self.fraction = 0.
        self.value = 0
        self.tuples = []

    def point(self, t, value):
        t0 = int(t / self.interval) * self.interval
        fraction = t - t0

        if t < self.t0 + self.fraction:
            raise ValueError('time t is in the past')

        if self.t0 == 0:
            # initial bin
            self.t0 = t0
            self.fraction = 0.

        if t0 == self.t0:
            # we are in the same bin as before
            # add in an appropriate amount of value
            delta = (fraction - self.fraction)/self.interval
            self.value += value * delta
            self.fraction = fraction
        elif t0 > self.t0:
            # we are in a future bin.
            # finish off the previous bin, push it as a tuple
            delta = (self.interval - self.fraction)/self.interval
            self.value += value * delta
            self.tuples.append((self.t0, self.value))
            # make 0+ intermediate bins
            while self.t0 + self.interval + 0.0001 < t0:
                self.t0 += self.interval
                self.tuples.append((self.t0, value))
            # make a final bin
            self.t0 = t0
            self.fraction = fraction
            delta = fraction / self.interval
            self.value = value * delta

    def gettuples(self, path=None):
        tuples = self.tuples
        self.tuples = []

        if path:
            return [(path, t) for t in tuples]
        else:
            return tuples
