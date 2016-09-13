from pytest import approx

import accumulator

def test_event_accumulator():
    A = accumulator.EventAccumulator()
    assert A.read() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    A.accumulate(1)
    assert A.read() == [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]

    A.accumulate(2.0)
    assert A.read() == [1.5, 1.5, 1.5, 1.5, 1.5, 1.5]

    [A.accumulate(x) for x in (2.0,)*8]
    assert A.read() == [1.9, 1.9, 1.9, 1.9, 1.9, 1.9]

    A.accumulate(2.0)
    assert A.read() == [2.0, 1.9, 1.9, 1.9, 1.9, 1.9]

    [A.accumulate(x) for x in (2.0,)*8]
    assert A.read() == [2.0, 1.9, 1.9, 1.9, 1.9, 1.9]
    A.accumulate(2.0)
    assert A.read() == [2.0, 1.9, 1.9, 1.9, 1.9, 1.9]

    # we don't expect it to change until we have 100 values
    [A.accumulate(x) for x in (2.0,)*100]
    assert A.read() == [2.0, approx(1.99), approx(1.99), approx(1.99), approx(1.99), approx(1.99)]

    A = accumulator.EventAccumulator()
    [A.accumulate(x) for x in range(0, 110)]
    assert A.read() == [104.5, 49.5, 49.5, 49.5, 49.5, 49.5]

    A = accumulator.EventAccumulator()
    [A.accumulate(x) for x in range(0, 1100)]
    assert A.read() == [1094.5, 1049.5, 499.5, 499.5, 499.5, 499.5]

    A = accumulator.EventAccumulator(function='max')
    A.accumulate(1.0)
    assert A.read() == [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    A.accumulate(2.0)
    assert A.read() == [2.0, 2.0, 2.0, 2.0, 2.0, 2.0]
    A.accumulate(2.0)
    assert A.read() == [2.0, 2.0, 2.0, 2.0, 2.0, 2.0]

    [A.accumulate(2.0) for x in range(0, 100)]
    assert A.read() == [2.0, 2.0, 2.0, 2.0, 2.0, 2.0]

    [A.accumulate(1.0) for x in range(0, 20)]
    assert A.read() == [1.0, 2.0, 2.0, 2.0, 2.0, 2.0]



