const LOGSTART = Ref{UInt64}(0)

"""
    ShortString("foo")

Represents a string that's short enough to fit entirely in a UInt128.
We take advantage of that by doing arithmetic on the UInt128 for
enumerating the decimal representation of the line numbers.
"""
struct ShortString
  val :: UInt128
  len :: Int
end

ShortString(s::String) = begin
  @assert length(s) <= sizeof(UInt128)
  s_padded = s * "\0" ^ sizeof(UInt128)
  val = unsafe_load(Ptr{UInt128}(pointer(s_padded)))
  ShortString(val, length(s))
end

Base.length(s::ShortString) = s.len

Base.:+(s::ShortString, x::Integer) = ShortString(s.val + x, s.len)
Base.:-(a::ShortString, b::ShortString) = begin
  @assert length(a) == length(b)
  a.val - b.val
end

concat(s::ShortString, a::Char) = begin
  newval = (s.val << 8) | UInt8(a)
  ShortString(newval, s.len + 1)
end

"""
    StaticBuffer(size)

Represents a simple byte array together with its next index.

This struct is non-mutable, and instead of updating `nextindex` in place, we
replace it with a new StaticBuffer (see the `put` implementation).
This has experimentally been much faster; I think the compiler can apply
more optimizations when it keeps the struct on the stack.
"""
struct StaticBuffer
  buf :: Vector{UInt8}
  nextindex :: Int
end

StaticBuffer(size) = StaticBuffer(Vector{UInt8}(undef, size), 1)

Base.length(buffer::StaticBuffer) = length(buffer.buf)
Base.pointer(buffer::StaticBuffer) = pointer(buffer.buf, buffer.nextindex)
Base.truncate(buffer::StaticBuffer) = StaticBuffer(buffer.buf, 1)

put(buffer::StaticBuffer, s::ShortString) = begin
  dest = Ptr{UInt128}(pointer(buffer))
  unsafe_store!(dest, s.val)
  StaticBuffer(buffer.buf, buffer.nextindex + s.len)
end

"""
    getpipefd!(io::IO)

Get a file descriptor (`::RawFD`) that is known to be a pipe; if `io`
isn't a pipe already, we insert a dummy `cat` process. This allows us
to use `vmsplice` which is much faster in the benchmark setup than `write`.
"""
getpipefd!(io::Base.PipeEndpoint) = Base._fd(io)
getpipefd!(io::Base.IOContext) = getpipefd!(io.io)
getpipefd!(io) = getpipefd!(open(pipeline(`cat`, stdout=io), write=true).in)

"""
    vmsplice(fdesc, buffer)

Splice the data in `buffer` to the pipe in `fdesc`.
"""
vmsplice(fdesc::RawFD, buffer::StaticBuffer) = begin
  ix = 1
  while ix < buffer.nextindex
    written = @ccall vmsplice(
      fdesc :: Cint,
      (pointer(buffer.buf, ix), buffer.nextindex - ix) :: Ref{Tuple{Ref{UInt8}, Csize_t}},
      1 :: Csize_t,
      0 :: Cuint) :: Cssize_t
    if written < 0
      error("Couldn't write to pipe")
    end
    ix += written
  end
end

"""
    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)

Move asciidigits and intdigits to the next line, i.e. add 1
to the ascii and decimal representations.
"""
@inline nextline(asciidigits, intdigits, plusone) = begin
  asciidigits += plusone
  intdigits = Base.setindex(intdigits, intdigits[1] + 1, 1)
  asciidigits, intdigits
end

const CARRY = ShortString("20") - ShortString("1:")

"""
    asciidigits, plusone, pluscarry = carry(position, asciidigits, plusone, pluscarry)

Perform a carry operation on asciidigits in the `position`th decimal place.
"""
@inline carry(position, asciidigits, plusone, pluscarry) = begin
  if position + 1 == length(asciidigits)
    asciidigits = concat(asciidigits, '0')

    logstr = string(
      "Number of digits is now $(length(asciidigits) - 1); ",
      "elapsed time is $((time_ns() - LOGSTART[]) / 10^9) s",
      "\n",
    )
    write(stderr, logstr)

    plusone <<= 8
    pluscarry = pluscarry .<< 8
    pluscarry = Base.setindex(pluscarry, CARRY, position)
  end
  asciidigits += pluscarry[position]
  asciidigits, plusone, pluscarry
end

"""
    @compiletime for a in b
      <statements>
    end

Unroll the loop.
"""
macro compiletime(forloop)
  @assert forloop.head == :for
  it, body = forloop.args
  @assert it.head == :(=)
  lhs, rhs = it.args

  expressions = gensym(:expressions)

  body = quote
    push!($expressions, $(Expr(:quote, body)))
  end

  expressions = Core.eval(__module__, quote
    let $expressions = []
      for $lhs in $rhs
        $body
      end
      $expressions
    end
  end)

  return esc(quote
    $(expressions...)
  end)
end

"""
    asciidigits, intdigits, plusone, pluscarry = maybecarry(asciidigits, intdigits, plusone, pluscarry)

If necessary, perform a carry operation on asciidigits and intdigits.
"""
@inline maybecarry(asciidigits, intdigits, plusone, pluscarry) = begin
  asciidigits += plusone

  @compiletime for d in 1:16
    intdigits = Base.setindex(intdigits, intdigits[$d] + 1, $d)
    intdigits[$d] != 10 && @goto carried
    intdigits = Base.setindex(intdigits, 0, $d)
    asciidigits, plusone, pluscarry = carry($d, asciidigits, plusone, pluscarry)
  end

  intdigits = Base.setindex(intdigits, intdigits[17] + 1, 17)
  intdigits[17] >= 10 && error("too big!")

  @label carried
  asciidigits, intdigits, plusone, pluscarry
end

const FIZZ = ShortString("Fizz\n")
const BUZZ = ShortString("Buzz\n")
const FIZZBUZZ = ShortString("FizzBuzz\n")

"""
  buffer = fizzbuzz(buffer::StaticBuffer, range::UnitRange)

Write the fizzbuzz output for line numbers in `range` to
`buffer`. It expects that range is of the form

    15i + 1 : 15j

for some values of `i <= j`.
"""
fizzbuzz(buffer::StaticBuffer, range::UnitRange) = begin
  iterations, remainder = divrem(length(range), 15)
  @assert remainder == 0
  @assert rem(first(range), 15) == 1
  @assert length(buffer) >= length(range) * sizeof(UInt128)

  lo = first(range)

  intdigits = digits(lo, pad=17)
  intdigits = tuple(intdigits...)::NTuple{17, Int}

  asciidigits = ShortString("$lo\n")
  numdigits = length(asciidigits) - 1

  plusone = UInt128(1) << 8(numdigits - 1)

  pluscarry = ntuple(Val(sizeof(UInt128))) do d
    if d < numdigits
      CARRY << 8(numdigits - d - 1)
    else
      UInt128(0)
    end
  end

  @GC.preserve buffer for _ in 1:iterations
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, FIZZ)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits, plusone, pluscarry = maybecarry(asciidigits, intdigits, plusone, pluscarry)
    buffer = put(buffer, BUZZ)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, FIZZ)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, FIZZ)

    asciidigits, intdigits, plusone, pluscarry = maybecarry(asciidigits, intdigits, plusone, pluscarry)
    buffer = put(buffer, BUZZ)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, FIZZ)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
    buffer = put(buffer, asciidigits)

    asciidigits, intdigits, plusone, pluscarry = maybecarry(asciidigits, intdigits, plusone, pluscarry)
    buffer = put(buffer, FIZZBUZZ)

    asciidigits, intdigits = nextline(asciidigits, intdigits, plusone)
  end
  buffer
end

const BUFSIZE = 100 * 4096

"""
  fizzbuzz(io::IO, cutoff=typemax(Int))

Write the fizzbuzz output to `io`. This will spawn `Threads.nthreads()`
tasks to maximize throughput through parallellism.

The `cutoff` parameter is approximate; depending on buffering, more lines
may be written to `io`.
"""
fizzbuzz(io::IO, cutoff=typemax(Int)) = begin
  LOGSTART[] = time_ns()

  buffers = [StaticBuffer(BUFSIZE) for _ in 1:Threads.nthreads()]

  fdesc = getpipefd!(io)

  nextline = 1
  chunklen = div(BUFSIZE, sizeof(UInt128))
  chunklen -= rem(chunklen, 15)

  tasks = [@Threads.spawn(fizzbuzz(buf, 1:0)) for buf in buffers]

  while nextline <= cutoff
    for t in eachindex(tasks)
      buffer = fetch(tasks[t])
      vmsplice(fdesc, buffer)

      lo = nextline
      hi = nextline + chunklen - 1
      tasks[t] = @Threads.spawn fizzbuzz(truncate(buffers[t]), lo:hi)
      nextline += chunklen
    end
  end
end

if abspath(PROGRAM_FILE) == @__FILE__
  fizzbuzz(stdout)
end
