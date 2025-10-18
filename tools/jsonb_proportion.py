#!/usr/bin/env python

# This shows the proportion an n-byte sequence that are valid JSONB.

# For n-byte of 1 through 5 I originally did the calculation by brute
# force by iterating every possible value.  Each one took 256 times
# longer than the previous!  The code below does random samples at
# each length and rapidly gives the same answers.

import random
import apsw

# The lengths we check
low = 1
high = 20

# How many tests are done for each iteration
count = 1
# Increment for next iteration
incr = 0.1

# Keep track of (valid jsonb, probes done) for each n_byte
passed = {n: (0, 0) for n in range(low, high + 1)}

# keep going forever doing more tests each time
while True:
    # show how many will have been for the values printed this time -
    # previous total plus increment this time
    print(f"\nProbes {passed[1][1] + count:,}")
    for n_byte in range(low, high + 1):
        yes, tot = passed[n_byte]
        yes += sum(apsw.jsonb_detect(random.randbytes(n_byte)) for _ in range(count))
        tot += count
        passed[n_byte] = (yes, tot)
        print(f"{n_byte:2d} {yes / tot:.4%}")

    # do more probes in the next round
    count += int(max(1, count * incr))

    # In theory we could adjust the low here.  eg there are only 256 possible sequences
    # for one byte and 65,536 for two bytes so there isn't any point doing more than that
    # for them.  But the shorter sequences calculate so fast it doesn't matter.
