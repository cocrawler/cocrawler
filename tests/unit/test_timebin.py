from pytest import approx

from cocrawler.timebin import TimeBin


def test_timebin():

    timebin1 = TimeBin(1.0)
    t = 1234567890.0  # I remember this time, it was a good time

    timebin1.point(t + 0.1, 100.)
    print(timebin1.value)
    assert timebin1.value == approx(10.)

    timebin1.point(t + 0.6, 200)
    assert timebin1.value == approx(110.)

    timebin1.point(t + 1.1, 100)
    assert timebin1.value == approx(10.)

    tuples = timebin1.gettuples('a path')
    assert len(tuples) == 1
    assert tuples[0][0] == 'a path'
    assert tuples[0][1] == approx((t, 150.))

    timebin30 = TimeBin(30.0)
    # t happens to be divisible by 30.
    timebin30.point(t + 7.5, 100.)
    assert timebin30.value == approx(25.0)

    timebin30.point(t + 75, 200.)
    assert timebin30.value == approx(100.0)

    tuples = timebin30.gettuples()
    assert len(tuples) == 2
    assert tuples[0] == approx((t, 175.))
    assert tuples[1] == approx((t + 30., 200.))
