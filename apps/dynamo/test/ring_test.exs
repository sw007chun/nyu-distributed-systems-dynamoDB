defmodule RingTest do
  use ExUnit.Case, async: true

  test "sequence" do
    index1 = 365375409332725729550921208179070754913983135744
    index2 = 730750818665451459101842416358141509827966271488
    ring_a = Ring.new_ring(4, :a)
    ring_b1 = %{ring_a | node_name: :b}
    ring_b2 = Ring.transfer_node(index1, :b, ring_b1)
    # transfers ownership to the same node
    assert ring_b2 == Ring.transfer_node(index1, :b, ring_b2)

    {:no_change, ring_a1} = Ring.reconcile(ring_b1, ring_a)

    ring_c1 = %{ring_a | node_name: :c}
    ring_c2 = Ring.transfer_node(index1, :c, ring_c1)
    {:new_ring, ring_a2} = Ring.reconcile(ring_c2, ring_a1)
    {:new_ring, ring_a3} = Ring.reconcile(ring_b2, ring_a2)

    ring_c3 = Ring.transfer_node(index2, :c, ring_c2)

    {:new_ring, ring_c4} = Ring.reconcile(ring_a3, ring_c3)
    {:new_ring, ring_a4} = Ring.reconcile(ring_c4, ring_a3)
    {:new_ring, ring_b3} = Ring.reconcile(ring_a4, ring_b2)

    assert ring_a4.chring == ring_b3.chring
    assert ring_b3.chring == ring_c4.chring
  end

  test "index" do
    ring0 = Ring.new_ring(2, Node.self())
    ring1 = Ring.transfer_node(0, :x, ring0)

    assert 0 == Ring.random_other_index(ring0)
    assert 0 == Ring.random_other_index(ring1)
    assert Node.self() == Ring.index_owner(ring0, 0)
    assert :x == Ring.index_owner(ring1, 0)
  end

  test "reconcile" do
    ring0 = Ring.new_ring(2, Node.self())
    ring1 = Ring.transfer_node(0, :x, ring0)
    {:new_ring, ring2} = Ring.reconcile(Ring.new_ring(2, :someone_else), ring1)

    assert ring1 != ring2

    ring3 = %{ring1 | node_name: :b}

    {:no_change, _} = Ring.reconcile(ring1, ring3)
  end

  test "membership" do
    ring_a1 = Ring.new_ring(:node_a)
    assert [:node_a] == Ring.all_members(ring_a1)

    ring_a2 = Ring.add_member(:node_a, ring_a1, :node_b)
    ring_a3 = Ring.add_member(:node_a, ring_a2, :node_c)
    assert [:node_a, :node_b, :node_c] == Ring.all_members(ring_a3)
    assert :joining == Ring.member_status(ring_a3, :node_c)

    # remove :node_c by making its status as :invalid
    ring_a4 = Ring.remove_member(:node_a, ring_a3, :node_c)
    assert [:node_a, :node_b] == Ring.all_members(ring_a4)

    # ring_a4.vclock > ring_a3.vclock thsu ring_a5 == ring_a4
    {_, ring_a5} = Ring.reconcile(ring_a3, ring_a4)
    assert [:node_a, :node_b] == Ring.all_members(ring_a5)

    # add node_b to node_c in ring_a3
    # but ring_a5.vclock > ring_b1.vclock
    # thus :node_c not added during reconciliation
    ring_b1 = Ring.add_member(:node_b, ring_a3, :node_c)
    {_, ring_a6} = Ring.reconcile(ring_b1, ring_a5)
    assert [:node_a, :node_b] == Ring.all_members(ring_a6)

    # :node_c added because ring_b2.vclock > ring_a6.vclcok
    ring_b2 = Ring.add_member(:node_b, ring_a6, :node_c)
    {_, ring_a7} = Ring.reconcile(ring_b2, ring_a6)
    assert [:node_a, :node_b, :node_c] == Ring.all_members(ring_a7)

    priority = [invalid: 1, down: 2, joining: 3, valid: 4, exiting: 5, leaving: 6]

    # concurrent status changes merged based on priority in merge_status/2
    for {status_a, priority_a} <- priority,
        {status_b, priority_b} <- priority do
      ring_t1 = Ring.set_member(:node_a, ring_a3, :node_c, status_a)
      assert status_a == Ring.member_status(ring_t1, :node_c)

      ring_t2 = Ring.set_member(:node_b, ring_a3, :node_c, status_b)
      assert status_b == Ring.member_status(ring_t2, :node_c)

      status_c =
        case priority_a < priority_b do
          true -> status_a
          false -> status_b
        end

      {_, ring_t3} = Ring.reconcile(ring_t2, ring_t1)
      assert status_c == Ring.member_status(ring_t3, :node_c)
    end

    # merge concurrent rings with lastest status of each nodes
    for {status_a, _} <- priority,
        {status_b, _} <- priority do
      ring_t1 = Ring.set_member(:node_a, ring_a3, :node_c, status_a)
      assert status_a == Ring.member_status(ring_t1, :node_c)

      ring_t2 = Ring.set_member(:node_b, ring_t1, :node_c, status_b)
      assert status_b == Ring.member_status(ring_t2, :node_c)

      ring_t3 = Ring.set_member(:node_a, ring_t1, :node_a, :valid)
      {_, ring_t4} = Ring.reconcile(ring_t2, ring_t3)

      assert status_b == Ring.member_status(ring_t4, :node_c)
    end
  end
end