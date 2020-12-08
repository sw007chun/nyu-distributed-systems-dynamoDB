defmodule Ring do
  @moduledoc """
  Actual implementation of ring
  """

  defstruct(
    node_name: nil,
    vclock: nil,
    chring: nil,
    members: %{},
    claimant: nil
  )

  @type chash_node() :: term()
  @type index_as_int() :: integer()
  @type node_entry() :: {index_as_int(), chash_node()}
  @type node_name() :: atom()
  @type members() :: %{node_name() => {member_status(), VClock}}
  @type member_status() :: :joining | :valid | :invalid | :leaving | :exiting | :down
  @type ring :: Ring

  @doc """
  Make new ring with current node as the seed node
  """
  def new_ring do
    new_ring(Node.self())
  end

  @doc """
  Make new ring with current node as the seed node.
  If ring size is unspecified, import from config.exs file
  """
  def new_ring(node_name) do
    new_ring(Application.get_env(:dynamo, :ring_size), node_name)
  end

  @spec new_ring(non_neg_integer(), node_name()) :: %Ring{}
  def new_ring(ring_size, node_name) do
    vclock = VClock.increment(node_name, VClock.new_clock())

    %Ring{
      node_name: node_name,
      vclock: vclock,
      chring: CHash.new_ring(ring_size, node_name),
      members: %{node_name => {:valid, vclock}},
      claimant: node_name
    }
  end

  @spec ring_size(ring()) :: non_neg_integer()
  def ring_size(ring) do
    CHash.size(ring.chring)
  end

  @spec num_partitions(ring()) :: non_neg_integer()
  def num_partitions(ring) do
    CHash.size(ring.chring)
  end

  @spec all_indices(%Ring{}) :: [index_as_int()]
  def all_indices(ring) do
    CHash.all_indices(ring.chring)
  end

  @spec add_member(ring(), ring(), node_name()) :: ring()
  def add_member(node, ring, member) do
    set_member(node, ring, member, :joining)
  end

  @spec leave_member(ring(), ring(), node_name()) :: ring()
  def leave_member(node, ring, member) do
    set_member(node, ring, member, :leaving)
  end

  @spec remove_member(ring(), ring(), node_name()) :: ring()
  def remove_member(node, ring, member) do
    set_member(node, ring, member, :invalid)
  end

  @doc """
  Increment node's counter in ring and member's vclock
  and change member's status
  """
  @spec set_member(node_name(), ring(), node_name(), atom()) :: ring()
  def set_member(node, ring, member, status) do
    my_vclock = VClock.increment(node, ring.vclock)
    default_clock = {status, VClock.increment(node, VClock.new_clock())}

    new_members =
      ring.members
      |> Map.update(
        member,
        default_clock,
        fn {_status, vclock} ->
          {status, VClock.increment(node, vclock)}
        end
      )

    %{ring | vclock: my_vclock, members: new_members}
  end

  @spec set_owner(ring(), node_name()) :: ring()
  def set_owner(ring, node) do
    %{ring | node_name: node}
  end

  @spec get_members(members()) :: [node_name()]
  defp get_members(members) do
    get_members(members, [:joining, :valid, :leaving, :exiting, :down])
  end

  @spec get_members(members(), [member_status()]) :: [node_name()]
  defp get_members(members, types) do
    for {node, {status, _vclock}} <- members, Enum.member?(types, status) do
      node
    end
  end

  @spec active_members(%Ring{}) :: [node_name()]
  def active_members(ring) do
    get_members(ring.members, [:joining, :valid, :leaving, :exiting])
  end

  @spec all_members(%Ring{}) :: [node_name()]
  def all_members(ring) do
    get_members(ring.members)
  end

  @spec claiming_members(%Ring{}) :: [node_name()]
  def claiming_members(ring) do
    get_members(ring.members, [:joining, :valid, :down])
  end

  @spec members(%Ring{}, [member_status()]) :: [node_name()]
  def members(ring, types) do
    get_members(ring.members, types)
  end

  @doc """
  Return all the node entries
  """
  @spec all_index_owners(%Ring{}) :: [node_entry()]
  def all_index_owners(ring) do
    CHash.nodes(ring.chring)
  end

  @spec index_owner(ring(), index_as_int()) :: node_name()
  def index_owner(ring, index) do
    {^index, owner} = List.keyfind(all_index_owners(ring), index, 0)
    owner
  end

  @doc """
  Return all the indices of partitions in the ring
  """
  @spec my_indices(%Ring{}) :: [index_as_int()]
  def my_indices(ring) do
    for {index, owner_node} <- all_index_owners(ring),
        owner_node === Node.self() do
      index
    end
  end

  @spec owner_node(%Ring{}) :: atom()
  def owner_node(ring) do
    ring.node_name
  end

  def claimant(ring) do
    ring.claimant
  end

  def set_claimant(ring, node) do
    %{ring | claimant: node}
  end

  @doc """
  Print partition info of the ring
  """
  @spec print_status(%Ring{}) :: none()
  def print_status(ring) do
    nodes = all_members(ring)
    node_entries = all_index_owners(ring)

    ring_size = ring_size(ring)
    initials = for i <- 97..(96 + length(nodes)), do: <<i>>
    node_initial_pair = Enum.zip(nodes, initials)
    split_size = 4

    header = "==================================== Nodes ===================================="

    node_info =
      for {node, initial} <- node_initial_pair do
        num_vnodes =
          node_entries
          |> Enum.reduce(0, fn {_, index_owner}, acc ->
            if index_owner === node, do: acc + 1, else: acc
          end)

        ring_percentage = num_vnodes / ring_size * 100
        "Node #{initial}: #{num_vnodes} (#{Float.round(ring_percentage, 1)}%) #{node}"
      end

    node_info = [header | node_info]

    header = "==================================== Ring ====================================="

    node_distribution =
      for {{_, node}, i} <- Enum.with_index(node_entries, 1) do
        initial = Keyword.fetch!(node_initial_pair, node)

        if rem(i, split_size) == 0 do
          initial <> "|"
        else
          initial
        end
      end

    node_distribution = [header | [Enum.join(node_distribution, "")]]

    (node_info ++ node_distribution)
    |> Enum.join("\n")
  end

  @spec member_status(%Ring{}, node_name()) :: member_status()
  def member_status(%Ring{members: members}, node) do
    member_status(members, node)
  end

  @spec member_status([members()], node_name()) :: member_status()
  def member_status(members, node) do
    case Map.get(members, node) do
      {status, _vclock} ->
        status

      _ ->
        :invalid
    end
  end

  # Merge two concurrent memeber status base on priority
  @spec merge_status(member_status(), member_status()) :: member_status()
  def merge_status(status1, status2) do
    case {status1, status2} do
      {:invalid, _} -> :invalid
      {_, :invalid} -> :invalid
      {:down, _} -> :down
      {_, :down} -> :down
      {:joining, _} -> :joining
      {_, :joining} -> :joining
      {:valid, _} -> :valid
      {_, :valid} -> :valid
      {:exiting, _} -> :exiting
      {_, :exiting} -> :exiting
      {:leaving, _} -> :leaving
      {_, :leaving} -> :leaving
      _ -> :invalid
    end
  end

  @doc """
  Return a random partition index not owned by this node.
  Otherwise return any index.
  """
  @spec random_other_index(ring()) :: index_as_int()
  def random_other_index(ring) do
    my_node = Node.self()

    not_owned_node_indices =
      for {index, owner} <- all_index_owners(ring), owner != my_node do
        index
      end

    case not_owned_node_indices do
      [] -> 0
      _ -> Enum.random(not_owned_node_indices)
    end
  end

  @doc """
  Reconcile two rings based on vector clocks.
  """
  @spec reconcile(%Ring{}, %Ring{}) :: {atom(), %Ring{}}
  def reconcile(ring1, ring2) do
    my_node = owner_node(ring1)
    merged_vclock = VClock.merge_vclocks(ring1.vclock, ring2.vclock)

    case VClock.compare_vclocks(ring1.vclock, ring2.vclock) do
      :before ->
        {:new_ring, %{ring2 | vclock: merged_vclock}}

      :after ->
        {:new_ring, %{ring1 | vclock: merged_vclock}}

      :equal ->
        {:no_change, ring1}

      :concurrent ->
        new_ring = reconcile_divergent(my_node, ring1, ring2)
        {:new_ring, %{new_ring | node_name: my_node}}
    end
  end

  @doc """
  Reconcile two concurrent rings
  """
  @spec reconcile_divergent(node_name(), %Ring{}, %Ring{}) :: %Ring{}
  def reconcile_divergent(node, ring1, ring2) do
    new_vclock = VClock.increment(node, VClock.merge_vclocks(ring1.vclock, ring2.vclock))
    new_members = reconcile_members(ring1, ring2)
    # arbitrary choice of ring1
    %{ring1 | vclock: new_vclock, members: new_members}
  end

  # Merge two members list using vlocks
  @spec reconcile_members(%Ring{}, %Ring{}) :: members()
  def reconcile_members(ring1, ring2) do
    Map.merge(ring1.members, ring2.members, fn _k, {status1, vclock1}, {status2, vclock2} ->
      merged_vclock = VClock.merge_vclocks(vclock1, vclock2)

      case VClock.compare_vclocks(vclock1, vclock2) do
        :before ->
          {status2, merged_vclock}

        :after ->
          {status1, merged_vclock}

        _ ->
          {merge_status(status1, status2), merged_vclock}
      end
    end)
  end

  @doc """
  Transfers owner of the partition at index with a new node
  """
  @spec transfer_node(index_as_int(), node_name(), ring()) :: ring()
  def transfer_node(index, node, my_ring) do
    case CHash.lookup(index, my_ring.chring) do
      ^node ->
        my_ring

      _ ->
        my_node = my_ring.node_name
        new_vclock = VClock.increment(my_node, my_ring.vclock)
        new_chring = CHash.update_owner(index, node, my_ring.chring)
        %{my_ring | vclock: new_vclock, chring: new_chring}
    end
  end
end
