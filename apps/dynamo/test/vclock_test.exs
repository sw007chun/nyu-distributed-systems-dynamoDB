defmodule VClockTest do
  use ExUnit.Case

  test "merge_vclocks is correct" do
    assert VClock.merge_vclocks(
             %{a: 6, b: 2, c: 6},
             %{a: 1, b: 200, c: 6}
           ) == %{a: 6, b: 200, c: 6}

    assert VClock.merge_vclocks(%{a: 2}, %{b: 3}) ==
             %{a: 2, b: 3}
  end

  test "increment is correct" do
    assert VClock.increment(:a, %{a: 7, b: 22}) == %{a: 8, b: 22}
    assert VClock.increment(:c, %{a: 7, b: 22}) == %{a: 7, b: 22, c: 1}
  end

  test "compare_vclocks is correct" do
    assert VClock.compare_vclocks(%{a: 8, b: 6}, %{a: 7, b: 5}) == :after
    assert VClock.compare_vclocks(%{a: 7, b: 5}, %{a: 8, b: 6}) == :before
    assert VClock.compare_vclocks(%{a: 7, b: 5}, %{a: 7, b: 5}) == :equal
    assert VClock.compare_vclocks(%{a: 1, b: 2}, %{a: 2, b: 1}) == :concurrent
    assert VClock.compare_vclocks(%{a: 22}, %{b: 66}) == :concurrent
  end

  test "vclock functions" do
    a = VClock.new_clock()
    b = VClock.new_clock()
    a1 = VClock.increment(:a, a)
    b1 = VClock.increment(:b, b)
    :before = VClock.compare_vclocks(a, a1)
    :before = VClock.compare_vclocks(b, b1)
    :concurrent = VClock.compare_vclocks(a1, b1)

    a2 = VClock.increment(:a, a1)
    c = VClock.merge_vclocks(a2, b1)
    c1 = VClock.increment(:c, c)
    :after = VClock.compare_vclocks(c1, a2)
    :after = VClock.compare_vclocks(c1, b1)
    :before = VClock.compare_vclocks(b1, c1)
    :concurrent = VClock.compare_vclocks(b1, a1)
  end
end