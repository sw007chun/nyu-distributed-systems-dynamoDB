defmodule PersistenceTest do
  use ExUnit.Case, async: true

  test "Test put and get working" do
    storage = PersistentStore.init_store()
    storage = PersistentStore.put_value(1, "Hello", storage)
    value = PersistentStore.get_value(1, storage)
    assert value == "Hello"
  end

end
