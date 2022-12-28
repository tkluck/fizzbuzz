# High-throughput FizzBuzz in Julia

This is a submission for [high-througput FizzBuzz codegolf][codegolf]. The
objective is to generate the highest throughput for FizzBuzz possible. On my
personal laptop, this julia script reaches ~10GiB/s, or about triple the
throughput of

```sh cat /dev/zero | pv > /dev/null
```

## Usage


```bash
# For seeing it in action:
julia --threads 4 fizzbuzz.jl
# For benchmarking the throughput:
julia --threads 4 fizzbuzz.jl | pv > /dev/null
# For checking correctness:
diff <( julia --threads 4 fizzbuzz.jl | head -n 10000) <(
        seq 10000 | perl -nle'
          $_ % 15 or print "FizzBuzz" and next;
          $_ % 5 or print "Buzz" and next;
          $_ % 3 or print "Fizz" and next;
          print'
      )
```

## Credit

I should credit ais523 for the idea to use the `vmsplice` syscall. Theirs is
currently (December 2022) the fastest submission. I also took inspiration from
other solutions to unroll the main loop by 15. The rest of the ideas are my
own, although I'm obviously standing on the shoulders of the giants who created
Julia.


[codegolf]: https://codegolf.stackexchange.com/questions/215216/high-throughput-fizz-buzz/236630#236630
