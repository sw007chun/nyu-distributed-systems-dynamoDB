defmodule VclockTest do
  use ExUnit.Case

  test "merge_vclocks is correct" do
    assert Vclock.merge_vclocks(
             %{a: 6, b: 2, c: 6},
             %{a: 1, b: 200, c: 6}
           ) == %{a: 6, b: 200, c: 6}

    assert Vclock.merge_vclocks(%{a: 2}, %{b: 3}) ==
             %{a: 2, b: 3}

    assert Vclock.merge_vclocks(%{}, %{a: 3, b: 3}) == %{a: 3, b: 3}
    assert Vclock.merge_vclocks(%{a: 2}, %{a: 3, b: 3}) == %{a: 3, b: 3}
    assert Vclock.merge_vclocks(%{a: 2, b: 1}, %{a: 3, b: 3}) == %{a: 3, b: 3}
  end

  test "increment is correct" do
    assert Vclock.increment(%{a: 7, b: 22}, :a) == %{a: 8, b: 22}
    assert Vclock.increment(%{a: 7, b: 22}, :c) == %{a: 7, b: 22, c: 1}
  end

  test "compare_vclocks is correct" do
    assert Vclock.compare_vclocks(%{a: 8, b: 6}, %{a: 7, b: 5}) == :after
    assert Vclock.compare_vclocks(%{a: 7, b: 5}, %{a: 8, b: 6}) == :before
    assert Vclock.compare_vclocks(%{a: 7, b: 5}, %{a: 7, b: 5}) == :equal
    assert Vclock.compare_vclocks(%{a: 1, b: 2}, %{a: 2, b: 1}) == :concurrent
    assert Vclock.compare_vclocks(%{a: 22}, %{b: 66}) == :concurrent
  end

  test "vclock functions" do
    a = Vclock.new_clock()
    b = Vclock.new_clock()
    a1 = Vclock.increment(a, :a)
    b1 = Vclock.increment(b, :b)
    :before = Vclock.compare_vclocks(a, a1)
    :before = Vclock.compare_vclocks(b, b1)
    :concurrent = Vclock.compare_vclocks(a1, b1)

    a2 = Vclock.increment(a1, :a)
    c = Vclock.merge_vclocks(a2, b1)
    c1 = Vclock.increment(c, :c)
    :after = Vclock.compare_vclocks(c1, a2)
    :after = Vclock.compare_vclocks(c1, b1)
    :before = Vclock.compare_vclocks(b1, c1)
    :concurrent = Vclock.compare_vclocks(b1, a1)
  end

  test "get latest clocks" do
    assert Vclock.get_latest_vclocks([
             %{a: 1, b: 0, c: 0},
             %{a: 1, b: 0, c: 0},
             %{a: 2, b: 0, c: 0}
           ]) ==
             [%{a: 2, b: 0, c: 0}]

    assert Vclock.get_latest_vclocks([
             %{a: 1, b: 0, c: 0},
             %{a: 1, b: 1, c: 0},
             %{a: 2, b: 0, c: 0}
           ])
           |> Enum.sort() ==
             [%{a: 1, b: 1, c: 0}, %{a: 2, b: 0, c: 0}]

    assert Vclock.get_latest_vclocks([
             %{a: 1, b: 0, c: 1},
             %{a: 1, b: 1, c: 0},
             %{a: 2, b: 0, c: 0}
           ])
           |> Enum.sort() ==
             [%{a: 1, b: 0, c: 1}, %{a: 1, b: 1, c: 0}, %{a: 2, b: 0, c: 0}]
  end
end
