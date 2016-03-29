import numpy

def test():
    buckets = [0] * 100000
    buckets[3] = 1
    buckets[4] = 3
    buckets[9] = 32


if __name__ == '__main__':

    import timeit
    print(timeit.timeit("test()", setup="from __main__ import test" ,number=1))