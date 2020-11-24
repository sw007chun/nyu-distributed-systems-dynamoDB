defmodule VClock do

  @type vclock_node() :: term()
  @type counter() :: integer()

  @type vclock() :: [clock()]
  @type clock() :: {vclock_node(), counter()}

  @spec new_clock :: vclock()
  def new_clock do
    []
  end

  @doc """
  Increment node's counter by 1 or default to 1 if node is not in the vector clock
  """
  @spec increment(vclock_node(), vclock()) :: vclock()
  def increment(node, vclock) do
    Keyword.update(vclock, node, 1, &(&1 + 1))
  end

end
