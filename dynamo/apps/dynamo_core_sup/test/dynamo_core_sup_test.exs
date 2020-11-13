defmodule DynamoCoreSupTest do
  use ExUnit.Case
  doctest DynamoCoreSup

  test "greets the world" do
    assert DynamoCoreSup.hello() == :world
  end
end
