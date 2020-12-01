defmodule VClock do

  @type vclock_node() :: term()
  @type counter() :: integer()

  @type vclock() :: map()
  @type clock() :: {vclock_node(), counter()}

  @spec new_clock :: vclock()
  def new_clock do
    %{}
  end

  @spec merge_vclocks(map(), map()) :: map()
  def merge_vclocks(current, received) do
    Map.merge(current, received, fn _k, c, r -> max(c, r) end)
  end

  @spec compare_component(
          non_neg_integer(),
          non_neg_integer()
        ) :: :before | :after | :concurrent
  defp compare_component(c1, c2) do
    cond do
      c1 == c2 -> :concurrent
      c1 < c2 -> :before
      c1 > c2 -> :after
    end
  end

  @doc """
  Increment node's counter by 1 or default to 1 if node is not in the vector clock
  """
  @spec increment(vclock_node(), vclock()) :: vclock()
  def increment(node, vclock) do
    Map.update(vclock, node, 1, &(&1 + 1))
  end

  @spec make_vclocks_equal_length(map(), map()) :: map()
  defp make_vclocks_equal_length(v1, v2) do
    v1_add = for {k, _} <- v2, !Map.has_key?(v1, k), do: {k, 0}
    Map.merge(v1, Enum.into(v1_add, %{}))
  end

  @spec compare_vclocks(map(), map()) :: :before | :after | :equal | :concurrent
  def compare_vclocks(v1, v2) do
    # First make the vectors equal length.
    v1 = make_vclocks_equal_length(v1, v2)
    v2 = make_vclocks_equal_length(v2, v1)
    # `compare_result` is a list of elements from
    # calling `compare_component` on each component of
    # `v1` and `v2`. Given this list you need to figure
    # out whether
    compare_result =
      Map.values(
        Map.merge(v1, v2, fn _k, c1, c2 -> compare_component(c1, c2) end)
      )

    # This is implementation of ES1 in `Fidge '88` paper.
    # If all the elements are @before or @concurrent and at least one element is @before
    # the v1 occured before v2. Symmetric for @after
    # If it is neither @before nor @after, it means it's concurrent.
    cond do
      Enum.all?(compare_result, fn x -> x == :concurrent or x == :before end) and
          Enum.any?(compare_result, fn x -> x == :before end) ->
        :before

      Enum.all?(compare_result, fn x -> x == :concurrent or x == :after end) and
          Enum.any?(compare_result, fn x -> x == :after end) ->
        :after

      # Added equal vclock
      Enum.all?(compare_result, fn x -> x == :concurrent end) ->
        :equal

      true ->
        :concurrent
    end
  end


end
