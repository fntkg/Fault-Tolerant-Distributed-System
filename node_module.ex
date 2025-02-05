defmodule NodeModule do
  @moduledoc """
  A simple distributed node module implementing a heartbeat-based failure detection
  and leader (coordinator) election algorithm.

  Each node is identified by an index and a name (a tuple of atom and node identifier).
  The module sends heartbeat messages to detect coordinator failure. When a failure is
  detected, nodes initiate an election. The election algorithm sends election messages to
  the other nodes, and if no higher priority (or faster) response is received within a timeout,
  the node declares itself as the new coordinator.

  **Key Functions:**

    - `launch/1`: Starts the node by printing other nodes and starting the heartbeat loop.
    - `heartbeat/2`: Periodically sends heartbeat messages and checks for coordinator failure.
    - `elect_coordinator/1`: Initiates a coordinator election process.
    - `announce_coordinator/1`: Notifies all other nodes of the newly elected coordinator.
  """

  @doc """
  Retrieves a list of all nodes except the current node.

  ## Parameters
    - `self_index`: The index of the current node.

  ## Returns
    - A list of tuples representing the other nodes.
  """
  defp get_other_nodes(self_index) do
    # List of all nodes in the cluster
    nodes = [
      {:node0, :"node0@127.0.0.1"},
      {:node1, :"node1@127.0.0.1"},
      {:node2, :"node2@127.0.0.1"},
      {:node3, :"node3@127.0.0.1"},
      {:node4, :"node4@127.0.0.1"}
    ]

    # Remove the current node from the list based on its index
    List.delete_at(nodes, self_index)
  end

  @doc """
  Launches the node by printing the other nodes and starting the heartbeat loop.

  ## Parameters
    - `self_index`: The index of the current node.
  """
  def launch(self_index) do
    # Get list of other nodes (excluding self)
    other_nodes = get_other_nodes(self_index)
    IO.puts("Other nodes: #{inspect(other_nodes)}")

    # Start the heartbeat mechanism with an initial dummy coordinator (-1)
    heartbeat(-1, self_index)
  end

  @doc """
  Announces to all other nodes that a new coordinator has been elected.

  ## Parameters
    - `coordinator_index`: The index of the node that is now the coordinator.
  """
  defp announce_coordinator(coordinator_index) do
    # Get all nodes except the coordinator (assuming coordinator does not need to notify itself)
    Enum.each(get_other_nodes(coordinator_index), fn pid ->
      # Send a coordinator announcement message to each node
      send(pid, {:coordinator, coordinator_index})
    end)
  end

  @doc """
  Initiates a coordinator election process.

  The function sends an election message to all other nodes.
  If a response is received within 5 seconds, it relinquishes the coordinator role;
  otherwise, it declares itself as the new coordinator.

  ## Parameters
    - `self_index`: The index of the current node initiating the election.
  """
  defp elect_coordinator(self_index) do
    # Send election messages to all other nodes
    Enum.each(get_other_nodes(self_index), fn pid ->
      # The message includes the candidate's index and its process identifier (self())
      send(pid, {:election, self_index, self()})
    end)

    receive do
      # If any node sends back an acknowledgment, the current node steps down.
      {:ok} ->
        IO.puts("NODE #{self_index}: I will not be the coordinator")
        # Continue with the existing coordinator (unchanged)
        nil
    after
      5_000 ->
        # Timeout after 5 seconds: assume no higher-priority node is present
        IO.puts("NODE #{self_index}: Declaring myself coordinator")
        # Announce to other nodes that this node is now the coordinator
        announce_coordinator(self_index)
        self_index
    end
  end

  @doc """
  Maintains the heartbeat loop and handles incoming messages.

  The function sends a heartbeat message to one of the other nodes and waits for a reply.
  It handles different message types:

    - `:election`: Receives an election request and acknowledges it.
    - `:coordinator`: Receives an announcement of a new coordinator.
    - `:get_coordinator`: Replies with the current coordinator.
    - `:heartbeat`: Receives a heartbeat message (used for confirmation).

  If no valid message is received within 3 seconds, it assumes the coordinator has failed and
  starts an election.

  ## Parameters
    - `coordinator_index`: The current coordinator's index.
    - `self_index`: The index of the current node.
  """
  defp heartbeat(coordinator_index, self_index) do
    # Send a heartbeat message to one of the other nodes
    # Here, the last node from the list is chosen arbitrarily.
    send(List.last(get_other_nodes(self_index)), {:heartbeat, self()})

    # Wait for messages related to election, coordinator announcement, or heartbeat confirmation.
    new_coordinator =
      receive do
        # Handle incoming election request
        {:election, candidate_index, sender_pid} ->
          IO.puts("NODE #{self_index}: Received election request from #{candidate_index}")
          # Acknowledge the election request by replying with :ok
          send(sender_pid, {:ok})
          # Return the current coordinator index (unchanged)
          coordinator_index

        # Handle coordinator announcement
        {:coordinator, new_coordinator_index} ->
          IO.puts("NODE #{self_index}: New coordinator has been set")
          new_coordinator_index

        # Handle request for current coordinator information
        {:get_coordinator, sender_pid} ->
          send(sender_pid, {:coordinator, coordinator_index})
          # Keep the current coordinator index unchanged
          coordinator_index

        # Handle incoming heartbeat message
        {:heartbeat} ->
          coordinator_index
      after
        3_000 ->
          # If no message is received within 3 seconds, assume coordinator failure.
          IO.puts("NODE #{self_index}: Coordinator failure detected; starting election")
          elect_coordinator(self_index)
      end

    # Sleep for 1 second before the next heartbeat cycle.
    Process.sleep(1000)
    # Continue the heartbeat loop with the possibly updated coordinator.
    heartbeat(new_coordinator, self_index)
  end
end
