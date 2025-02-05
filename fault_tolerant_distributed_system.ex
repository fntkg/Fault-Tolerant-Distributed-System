defmodule Fib do
  @moduledoc """
  Module providing several implementations of the Fibonacci sequence.
  
  It includes:
  
    - A naive recursive version (`fibonacci/1`) which is simple but inefficient.
    - A tail-recursive version (`fibonacci_tail/1`) which is optimized for recursion.
    - An implementation using Binet's formula (`fibonacci_binet/1`), which uses
      floating-point arithmetic and the mathematical closed-form solution.
  """

  # Naive recursive Fibonacci (inefficient)
  @doc """
  Computes the Fibonacci number at position `n` using a naive recursive approach.

  ## Examples

      iex> Fib.fibonacci(0)
      0
      iex> Fib.fibonacci(1)
      1
      iex> Fib.fibonacci(5)
      5

  Note: This implementation has exponential time complexity.
  """
  def fibonacci(0), do: 0
  def fibonacci(1), do: 1
  def fibonacci(n) when n >= 2 do
    fibonacci(n - 2) + fibonacci(n - 1)
  end

  # Tail‐recursive Fibonacci
  @doc """
  Computes the Fibonacci number at position `n` using a tail-recursive approach.

  This is more efficient than the naive recursion as it avoids building up
  a large call stack.

  ## Examples

      iex> Fib.fibonacci_tail(0)
      0
      iex> Fib.fibonacci_tail(1)
      1
      iex> Fib.fibonacci_tail(10)
      55
  """
  def fibonacci_tail(n), do: fibonacci_tail(n, 0, 1)

  # Private helper function implementing tail recursion.
  defp fibonacci_tail(0, _prev, _curr), do: 0
  defp fibonacci_tail(1, _prev, curr), do: curr
  defp fibonacci_tail(steps_remaining, prev, curr) do
    fibonacci_tail(steps_remaining - 1, curr, prev + curr)
  end

  @sqrt_5 :math.sqrt(5)
  # Fibonacci via Binet's formula
  @doc """
  Computes the Fibonacci number at position `n` using Binet's formula.

  This approach uses floating-point arithmetic and the closed-form expression:
  
      F(n) = (phi^n - psi^n) / sqrt(5)
  
  where:
  
      phi = (1 + sqrt(5)) / 2
      psi = (1 - sqrt(5)) / 2

  ## Examples

      iex> round(Fib.fibonacci_binet(10))
      55
  """
  def fibonacci_binet(n) do
    (phi_power(n) - psi_power(n)) / @sqrt_5
  end

  # Private helper functions for Binet's formula.
  defp phi_power(n) do
    :math.pow((1 + @sqrt_5) / 2, n)
  end

  def psi_power(n) do
    :math.pow((1 - @sqrt_5) / 2, n)
  end
end

defmodule Worker do
  @moduledoc """
  Module that simulates a worker process responsible for handling Fibonacci
  computation requests.
  
  Each worker initially sleeps for 10 seconds (simulating initialization delay)
  and then enters a loop (`worker/3`) where it listens for requests and processes them.
  
  The worker occasionally changes its behavior to simulate node crashes,
  different Fibonacci implementations, or even omitting responses.
  """

  @doc """
  Initializes the worker process.

  The worker starts with a 10-second delay and then enters the processing loop,
  using the tail-recursive Fibonacci function by default.
  """
  def init do
    Process.sleep(10_000)
    # Start with the tail‐recursive Fibonacci function.
    worker(&Fib.fibonacci_tail/1, 1, :rand.uniform(10))
  end

  # Internal worker loop that handles requests and simulates varied behavior.
  defp worker(operation, request_counter, behavior_interval) do
    {new_operation, omit_response} =
      if rem(request_counter, behavior_interval) == 0 do
        # At specific intervals, the worker randomly selects a different behavior.
        behavior_chance = :rand.uniform(100)
        cond do
          behavior_chance >= 90 ->
            # Simulate a node crash by halting the system.
            {&System.halt/1, false}
          behavior_chance >= 70 ->
            # Switch to using the naive recursive Fibonacci function.
            {&Fib.fibonacci/1, false}
          behavior_chance >= 50 ->
            # Switch to using Binet's formula for Fibonacci computation.
            {&Fib.fibonacci_binet/1, false}
          behavior_chance >= 30 ->
            # Use the tail‐recursive Fibonacci function but omit the response.
            {&Fib.fibonacci_tail/1, true}
          true ->
            # Default behavior: tail‐recursive Fibonacci with a response.
            {&Fib.fibonacci_tail/1, false}
        end
      else
        {operation, false}
      end

    receive do
      # Wait for a request message with a client PID and an argument.
      {:req, {client_pid, argument}} ->
        # If not omitting a response, send the computed result back to the client.
        if not omit_response, do: send(client_pid, operation.(argument))
    end

    # Continue the loop with updated operation and request counter.
    worker(new_operation, rem(request_counter + 1, behavior_interval), behavior_interval)
  end
end

defmodule Client do
  @moduledoc """
  Module simulating client processes that generate requests for Fibonacci computations.
  
  Clients can launch either a single-threaded request or spawn additional threads
  to simulate concurrent requests.
  """

  # When there's a single thread, the client sends a fixed request value.
  defp launch(server_pid, 1) do
    send(server_pid, {self(), 1500})
    receive do
      {:result, result} -> result
    end
  end

  # When thread_count is greater than 1, the client spawns an additional thread.
  defp launch(server_pid, thread_count) when thread_count != 1 do
    IO.puts("CLIENT_#{inspect(thread_count)}: Spawning an additional thread")
    spawn(fn -> launch(server_pid, thread_count - 1) end)
    # The max random bound depends on the thread count.
    max_random_bound = if rem(thread_count, 3) == 0, do: 100, else: 36
    start_time = Time.utc_now()
    send(server_pid, {self(), :rand.uniform(max_random_bound)})
    receive do
      {:result, result} -> result
    end
    end_time = Time.utc_now()
    IO.puts("TIME: #{Time.diff(end_time, start_time, :millisecond)} milliseconds")
  end

  @doc """
  Continuously generates a workload by sending requests to the server (worker/proxy).

  After each request, it sleeps for a random period before issuing another request.
  """
  def generate_workload(server_pid) do
    launch(server_pid, 6 + :rand.uniform(2))
    Process.sleep(2000 + :rand.uniform(200))
    generate_workload(server_pid)
  end
end

defmodule WorkerPool do
  @moduledoc """
  Module managing a pool of worker nodes.

  The pool maintains a list of worker node names and provides functionality to:
  
    - Add a new worker to the pool.
    - Remove a worker from the pool (e.g., after assigning a task).
    - Retrieve a worker for handling a request.
  
  It also handles incoming messages to update or provide workers.
  """

  @doc """
  Creates the initial list of worker node names.

  In this example, three worker nodes (represented by atoms) are added.
  """
  def create_worker_list() do
    # One worker per node (example IPs/names)
    [:"worker@10.1.59.37", :"worker@10.1.59.37", :"worker@10.1.59.37"]
  end

  @doc """
  Adds a new worker to the current worker list.

  Prints the current list before adding the new worker.
  """
  def add_worker(new_worker, worker_list) do
    IO.inspect(worker_list, label: "WORKER POOL: Adding worker; current list:")
    worker_list ++ [new_worker]
  end

  @doc """
  Removes a worker from the pool (the first worker in the list).

  Prints the new list after removal.
  """
  def remove_worker([_removed | remaining_workers]) do
    IO.inspect(remaining_workers, label: "WORKER POOL: Removing worker; new list:")
    remaining_workers
  end

  @doc """
  Retrieves a worker from the pool (the first worker in the list).

  Also prints the current list of workers.
  """
  def get_worker([worker | _rest] = worker_list) do
    IO.inspect(worker_list, label: "WORKER POOL: Retrieving a worker from list:")
    worker
  end

  @doc """
  Starts the worker pool process, initializing the worker list and handling messages.
  """
  def start() do
    IO.puts("WORKER POOL: Started pool of workers")
    handle_messages(create_worker_list())
  end

  # Loop to handle messages sent to the worker pool.
  def handle_messages(worker_list) do
    receive do
      {:add, new_worker} ->
        # Add a worker to the pool.
        handle_messages(add_worker(new_worker, worker_list))

      {:request, requester_pid} ->
        # When a requester asks for a worker, provide one and remove it from the pool.
        send(requester_pid, {get_worker(worker_list), :available})
        handle_messages(remove_worker(worker_list))

      _unexpected ->
        IO.puts("WORKER POOL: Error – Unrecognized message")
        handle_messages(worker_list)
    end
  end
end

defmodule Master do
  @moduledoc """
  Module coordinating client requests and worker assignments.

  The Master process waits for messages from either clients or workers.
  
    - When it receives a client request, it asks the WorkerPool for an available worker,
      then forwards the client’s request to the Proxy.
    - When a worker finishes a task, the Master is notified and returns the worker
      back to the pool.
  """

  @doc """
  Main loop for the Master process.

  Depending on the incoming message, it:
  
    - Receives a client request, gets an available worker from the pool,
      and sends the task to the Proxy.
    - Receives a notification that a worker has completed a task, and returns
      the worker to the pool.
  """
  def run(proxy_pid, pool_pid) do
    receive do
      {client_pid, request_number} ->
        IO.puts("MASTER: Received a request from the client")
        IO.puts("MASTER: Requesting an available worker from the worker pool")
        send({:pool, pool_pid}, {:request, self()})
        receive do
          {available_worker, :available} ->
            IO.puts("MASTER: Received an available worker")
            IO.puts("MASTER: Forwarding client request to the proxy")
            send({:proxy, proxy_pid}, {client_pid, request_number, available_worker})
        end

      {worker_pid} ->
        IO.puts("MASTER: Worker completed task. Returning worker to pool")
        send({:pool, pool_pid}, {:add, worker_pid})
    end

    # Continue processing messages.
    run(proxy_pid, pool_pid)
  end
end

defmodule Proxy do
  @moduledoc """
  Module acting as an intermediary (proxy) between the Master and Worker processes.

  It receives requests from the Master, spawns a dedicated thread (process)
  to handle each client request, and manages retry logic and worker monitoring.
  """

  @doc """
  Main proxy loop that waits for forwarded requests from the Master.
  
  For each request, it spawns a new process (thread) to handle the request.
  """
  def proxy(worker_pool_pid, master_pid, request_id) do
    receive do
      {client_pid, request_value, worker_pid} ->
        IO.puts("PROXY: Received request from MASTER")
        IO.puts("PROXY: Spawning thread to handle request")
        spawn(fn ->
          handle_request(client_pid, request_value, worker_pid, worker_pool_pid, master_pid, request_id, 1)
        end)
        proxy(worker_pool_pid, master_pid, request_id + 1)
    end
  end

  @doc """
  Handles the client request by communicating with the assigned worker.

  It monitors the worker node, sends the request, and waits for a response.
  
  The function handles:
  
    - Valid responses from the worker.
    - Invalid response formats.
    - Worker node failures (monitored via `:nodedown`).
    - Timeout cases with a retry mechanism.
  
  If after 4 attempts there is no valid response, the worker is considered dead,
  and the client’s request is resubmitted.
  """
  def handle_request(client_pid, request_value, worker_pid, _worker_pool_pid, master_pid, request_id, attempt) do
    IO.puts("PROXY_THREAD#{inspect(request_id)}: Starting worker monitoring")
    Node.monitor(worker_pid, true)
    IO.puts("PROXY_THREAD#{inspect(request_id)}: Sending request to worker")
    send({:worker, worker_pid}, {:req, {self(), request_value}})
    IO.puts("PROXY_THREAD#{inspect(request_id)}: Waiting for worker response")
    
    receive do
      result ->
        IO.puts("PROXY_THREAD#{inspect(request_id)}: Received response from worker")
        IO.puts("PROXY_THREAD#{inspect(request_id)}: Validating response format")
        # Check response format (in this example, it expects an integer result)
        if rem(result, 1) != 0 do
          IO.puts("PROXY_THREAD#{inspect(request_id)}: Response format invalid.")
          IO.puts("PROXY_THREAD#{inspect(request_id)}: Resending request.")
          spawn(fn ->
            handle_request(client_pid, request_value, worker_pid, _worker_pool_pid, master_pid, request_id + 1, attempt)
          end)
        end
        IO.puts("PROXY_THREAD#{inspect(request_id)}: Forwarding response to client")
        send(client_pid, {:result, result})
        IO.puts("PROXY_THREAD#{inspect(request_id)}: Notifying master of worker completion")
        send({:master, master_pid}, {worker_pid})

      {:nodedown, _node} ->
        IO.puts("PROXY_THREAD#{inspect(request_id)}: Worker down: #{inspect(worker_pid)}")
        IO.puts("PROXY_THREAD#{inspect(request_id)}: Resubmitting client request")
        send({:master, master_pid}, {client_pid, request_value})

      after 2400 ->
        IO.puts("PROXY_THREAD#{inspect(request_id)}: No response within 2.4 seconds")
        IO.puts("PROXY_THREAD#{inspect(request_id)}: Resending request")
        if attempt == 4 do
          IO.puts("PROXY_THREAD#{inspect(request_id)}: Worker timeout reached")
          IO.puts("PROXY_THREAD#{inspect(request_id)}: Worker considered dead: #{inspect(worker_pid)}")
          IO.puts("PROXY_THREAD#{inspect(request_id)}: Resubmitting client request")
          send({:master, master_pid}, {client_pid, request_value})
        else
          spawn(fn ->
            handle_request(client_pid, request_value, worker_pid, _worker_pool_pid, master_pid, request_id + 1, attempt + 1)
          end)
        end
    end
  end
end
