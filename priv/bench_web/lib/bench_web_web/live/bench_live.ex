defmodule BenchWebWeb.BenchLive do
  use BenchWebWeb, :live_view

  @scenario_options [
    {"1", "1. CAS Put Latency"},
    {"2", "2. Consistent Read vs Eventual Read"},
    {"3", "3. Update Latency"},
    {"4", "4. Parallel CAS Throughput"},
    {"5", "5. Hot-Key Contention"},
    {"6", "6. CAS Replication Latency"},
    {"7", "7. Config Store Simulation"},
    {"8", "8. Session Lifecycle"},
    {"9", "9. CAS vs LWW Put Comparison"}
  ]

  @default_scenarios ~w(4 5 6 7 8 9)

  @impl true
  def mount(_params, _session, socket) do
    summary = cluster_summary()
    replicas = Enum.join(summary.replicas, ", ")
    cluster_size = Integer.to_string(summary.replica_count)
    data_root = default_data_root()
    scenarios = @default_scenarios
    quick = true

    socket =
      socket
      |> assign(:page_title, "EKV CAS Bench")
      |> assign(:running, false)
      |> assign(:task_ref, nil)
      |> assign(:run_ref, nil)
      |> assign(:output, nil)
      |> assign(:error, nil)
      |> assign(:current_phase, "Idle")
      |> assign(:scenario_options, @scenario_options)
      |> assign(:summary, summary)
      |> assign(:last_autofill_replicas, replicas)
      |> assign(:last_autofill_cluster_size, cluster_size)
      |> assign(:form, default_form(replicas, cluster_size, data_root, scenarios, quick))

    if connected?(socket) do
      Process.send_after(self(), :refresh_cluster, 1_000)
    end

    {:ok, socket}
  end

  @impl true
  def handle_event("validate", %{"bench" => params}, socket) do
    {:noreply, assign(socket, :form, to_form(params, as: :bench))}
  end

  @impl true
  def handle_event("run", %{"bench" => _params}, %{assigns: %{running: true}} = socket) do
    {:noreply, put_flash(socket, :error, "A benchmark run is already in progress")}
  end

  @impl true
  def handle_event("run", %{"bench" => params}, socket) do
    with {:ok, shards} <- parse_shards(params["shards"]),
         {:ok, replicas} <- parse_replicas(params["replicas"]),
         {:ok, cluster_size} <- parse_cluster_size(params["cluster_size"], length(replicas)),
         {:ok, data_root} <- parse_data_root(params["data_root"]),
         {:ok, scenarios} <- parse_scenarios(params["scenarios"]),
         {:ok, quick} <- parse_quick(params["quick"]) do
      run_ref = make_ref()
      owner = self()

      task =
        Task.Supervisor.async_nolink(BenchWeb.TaskSupervisor, fn ->
          BenchWeb.BenchRunner.run_cas_stream(
            [
              shards: shards,
              replicas: replicas,
              cluster_size: cluster_size,
              data_root: data_root,
              scenarios: scenarios,
              quick: quick
            ],
            owner,
            run_ref
          )
        end)

      {:noreply,
       socket
       |> assign(:running, true)
       |> assign(:task_ref, task.ref)
       |> assign(:run_ref, run_ref)
       |> assign(:error, nil)
       |> assign(:output, "")
       |> assign(:current_phase, "Starting benchmark...")
       |> assign(:form, to_form(params, as: :bench))}
    else
      {:error, reason} ->
        {:noreply,
         socket |> assign(:form, to_form(params, as: :bench)) |> put_flash(:error, reason)}
    end
  end

  @impl true
  def handle_info(:refresh_cluster, socket) do
    summary = cluster_summary()
    refreshed_replicas = Enum.join(summary.replicas, ", ")
    shards = form_value(socket.assigns.form, :shards, "8")
    replicas = form_value(socket.assigns.form, :replicas, "")
    cluster_size = form_value(socket.assigns.form, :cluster_size, "3")
    data_root = form_value(socket.assigns.form, :data_root, default_data_root())
    quick = checkbox_checked?(socket.assigns.form, :quick)
    scenarios = form_list_value(socket.assigns.form, :scenarios, @default_scenarios)
    refreshed_cluster_size = Integer.to_string(summary.replica_count)

    socket =
      socket
      |> assign(:summary, summary)
      |> maybe_autofill_form(
        shards,
        replicas,
        cluster_size,
        data_root,
        quick,
        scenarios,
        refreshed_replicas,
        refreshed_cluster_size
      )

    if connected?(socket) do
      Process.send_after(self(), :refresh_cluster, 2_000)
    end

    {:noreply, socket}
  end

  @impl true
  def handle_info(
        {:bench_progress, run_ref, chunk},
        %{assigns: %{running: true, run_ref: run_ref}} = socket
      ) do
    output = (socket.assigns.output || "") <> chunk
    phase = latest_phase(chunk, socket.assigns.current_phase)

    {:noreply, socket |> assign(:output, output) |> assign(:current_phase, phase)}
  end

  @impl true
  def handle_info({:bench_progress, _run_ref, _chunk}, socket) do
    {:noreply, socket}
  end

  @impl true
  def handle_info({ref, result}, %{assigns: %{task_ref: ref}} = socket) do
    Process.demonitor(ref, [:flush])

    socket =
      case result do
        {:ok, output} ->
          socket
          |> assign(:running, false)
          |> assign(:task_ref, nil)
          |> assign(:run_ref, nil)
          |> assign(:output, output)
          |> assign(:current_phase, "Completed")
          |> assign(:summary, cluster_summary())
          |> put_flash(:info, "Benchmark completed")

        {:error, reason, output} ->
          socket
          |> assign(:running, false)
          |> assign(:task_ref, nil)
          |> assign(:run_ref, nil)
          |> assign(:error, reason)
          |> assign(:output, output)
          |> assign(:current_phase, "Failed")
          |> assign(:summary, cluster_summary())
          |> put_flash(:error, "Benchmark failed")
      end

    {:noreply, socket}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{assigns: %{task_ref: ref}} = socket) do
    {:noreply,
     socket
     |> assign(:running, false)
     |> assign(:task_ref, nil)
     |> assign(:run_ref, nil)
     |> assign(:error, "Task exited: #{inspect(reason)}")
     |> assign(:current_phase, "Crashed")
     |> put_flash(:error, "Benchmark task crashed")}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.flash_group flash={@flash} />
    <section class="min-h-screen bg-zinc-950 text-zinc-100">
      <div class="mx-auto max-w-6xl px-4 py-10 sm:px-6">
        <div class="rounded-2xl border border-cyan-900/40 bg-zinc-900/70 p-6 shadow-[0_0_0_1px_rgba(8,145,178,0.08),0_20px_80px_rgba(0,0,0,0.45)] backdrop-blur">
          <div class="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p class="text-xs uppercase tracking-[0.24em] text-cyan-300/80">EKV CASPaxos Bench</p>
              <h1 class="mt-2 text-2xl font-semibold tracking-tight">Fly Orchestrator</h1>
              <p class="mt-2 text-sm text-zinc-400">
                Run distributed CAS benchmarks from this node across connected Fly machines.
              </p>
            </div>
            <div class="rounded-xl border border-zinc-800 bg-zinc-900 px-4 py-3 text-xs text-zinc-300">
              <p>coordinator: <span class="text-zinc-100">{@summary.coordinator}</span></p>
              <p class="mt-1">
                connected members: <span class="text-zinc-100">{@summary.connected_count}</span>
              </p>
              <p class="mt-1">
                discovered replicas: <span class="text-zinc-100">{@summary.replica_count}</span>
              </p>
              <p class="mt-1">
                phase: <span class="text-zinc-100">{@current_phase}</span>
              </p>
            </div>
          </div>

          <.form
            for={@form}
            id="bench-form"
            phx-change="validate"
            phx-submit="run"
            class="mt-8 grid gap-4 rounded-xl border border-zinc-800 bg-zinc-950/70 p-5"
          >
            <label class="grid gap-1">
              <span class="text-xs font-medium uppercase tracking-[0.18em] text-zinc-400">
                Shards
              </span>
              <input
                type="number"
                min="1"
                name={@form[:shards].name}
                value={@form[:shards].value}
                class="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm outline-none ring-cyan-500 transition focus:ring-2"
              />
            </label>

            <label class="grid gap-1">
              <span class="text-xs font-medium uppercase tracking-[0.18em] text-zinc-400">
                Replica nodes (comma or newline separated)
              </span>
              <textarea
                rows="4"
                name={@form[:replicas].name}
                class="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 font-mono text-xs outline-none ring-cyan-500 transition focus:ring-2"
              ><%= @form[:replicas].value %></textarea>
            </label>

            <label class="grid gap-1">
              <span class="text-xs font-medium uppercase tracking-[0.18em] text-zinc-400">
                Cluster size
              </span>
              <input
                type="number"
                min="3"
                name={@form[:cluster_size].name}
                value={@form[:cluster_size].value}
                class="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm outline-none ring-cyan-500 transition focus:ring-2"
              />
            </label>

            <label class="grid gap-1">
              <span class="text-xs font-medium uppercase tracking-[0.18em] text-zinc-400">
                Data root (volume-backed preferred)
              </span>
              <input
                type="text"
                name={@form[:data_root].name}
                value={@form[:data_root].value}
                class="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm font-mono outline-none ring-cyan-500 transition focus:ring-2"
              />
            </label>

            <div class="grid gap-2 rounded-lg border border-zinc-800 bg-zinc-900/40 p-3">
              <label class="flex items-center gap-2 text-sm text-zinc-300">
                <input
                  type="checkbox"
                  name={@form[:quick].name}
                  value="true"
                  checked={checkbox_checked?(@form, :quick)}
                  class="h-4 w-4 rounded border-zinc-600 bg-zinc-900 text-cyan-400 focus:ring-cyan-500"
                />
                Quick mode (lower iteration counts for faster feedback)
              </label>
            </div>

            <div class="grid gap-2 rounded-lg border border-zinc-800 bg-zinc-900/40 p-3">
              <p class="text-xs font-medium uppercase tracking-[0.18em] text-zinc-400">
                Scenarios
              </p>
              <div class="grid gap-2 md:grid-cols-2">
                <%= for {scenario_id, label} <- @scenario_options do %>
                  <label class="flex items-center gap-2 text-xs text-zinc-300">
                    <input
                      type="checkbox"
                      name="bench[scenarios][]"
                      value={scenario_id}
                      checked={scenario_checked?(@form, scenario_id)}
                      class="h-4 w-4 rounded border-zinc-600 bg-zinc-900 text-cyan-400 focus:ring-cyan-500"
                    />
                    {label}
                  </label>
                <% end %>
              </div>
            </div>

            <div class="flex items-center justify-between gap-4">
              <p class="text-xs text-zinc-500">
                Cluster size must be between 3 and listed replicas. Select at least one scenario.
              </p>
              <button
                type="submit"
                disabled={@running}
                class={[
                  "rounded-lg px-4 py-2 text-sm font-semibold transition",
                  if(@running,
                    do: "cursor-not-allowed bg-zinc-700 text-zinc-300",
                    else: "bg-cyan-500 text-zinc-950 hover:bg-cyan-400"
                  )
                ]}
              >
                {if @running, do: "Running...", else: "Run CAS Bench"}
              </button>
            </div>

            <p class="text-xs text-zinc-500">
              Discovered: {Enum.join(@summary.replicas, ", ")}
            </p>
          </.form>

          <div class="mt-6 grid gap-4">
            <%= if @error do %>
              <div class="rounded-lg border border-rose-800/60 bg-rose-950/40 px-4 py-3 text-sm text-rose-200">
                {@error}
              </div>
            <% end %>

            <div class="rounded-xl border border-zinc-800 bg-black/40 p-4">
              <p class="mb-3 text-xs uppercase tracking-[0.2em] text-zinc-500">Run Output</p>
              <pre class="max-h-[34rem] overflow-auto whitespace-pre-wrap font-mono text-xs leading-6 text-zinc-200"><%= @output || "No run yet." %></pre>
            </div>
          </div>
        </div>
      </div>
    </section>
    """
  end

  defp default_form(replicas, cluster_size, data_root, scenarios, quick) do
    to_form(
      %{
        "shards" => "8",
        "replicas" => replicas,
        "cluster_size" => cluster_size,
        "data_root" => data_root,
        "scenarios" => scenarios,
        "quick" => if(quick, do: "true", else: "false")
      },
      as: :bench
    )
  end

  defp parse_shards(nil), do: {:error, "shards is required"}

  defp parse_shards(value) do
    case Integer.parse(to_string(value)) do
      {n, ""} when n > 0 -> {:ok, n}
      _ -> {:error, "shards must be a positive integer"}
    end
  end

  defp parse_replicas(raw) do
    replicas =
      raw
      |> to_string()
      |> String.split([",", "\n", "\t", " "], trim: true)
      |> Enum.uniq()

    if length(replicas) < 3 do
      {:error, "at least 3 replicas are required"}
    else
      {:ok, replicas}
    end
  end

  defp parse_cluster_size(nil, _replica_count), do: {:error, "cluster_size is required"}

  defp parse_cluster_size(value, replica_count) do
    case Integer.parse(to_string(value)) do
      {n, ""} when n >= 3 and n <= replica_count -> {:ok, n}
      {n, ""} when n < 3 -> {:error, "cluster_size must be at least 3"}
      {n, ""} when n > replica_count -> {:error, "cluster_size cannot exceed replica count"}
      _ -> {:error, "cluster_size must be an integer"}
    end
  end

  defp parse_data_root(value) do
    root = value |> to_string() |> String.trim()

    cond do
      root == "" ->
        {:error, "data_root is required"}

      not String.starts_with?(root, "/") ->
        {:error, "data_root must be an absolute path"}

      true ->
        {:ok, root}
    end
  end

  defp parse_scenarios(nil), do: {:error, "select at least one scenario"}

  defp parse_scenarios(raw) do
    parsed =
      raw
      |> List.wrap()
      |> Enum.map(&to_string/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(fn scenario ->
        case Integer.parse(scenario) do
          {n, ""} when n in 1..9 -> {:ok, n}
          _ -> {:error, scenario}
        end
      end)

    case Enum.find(parsed, &match?({:error, _}, &1)) do
      {:error, invalid} ->
        {:error, "invalid scenario #{inspect(invalid)}"}

      nil ->
        scenarios =
          parsed
          |> Enum.map(fn {:ok, n} -> n end)
          |> Enum.uniq()
          |> Enum.sort()

        if scenarios == [] do
          {:error, "select at least one scenario"}
        else
          {:ok, scenarios}
        end
    end
  end

  defp parse_quick(value), do: {:ok, truthy?(value)}

  defp cluster_summary do
    connected_peers = Node.list()

    replicas =
      [node() | connected_peers]
      |> Enum.uniq()
      |> Enum.map(&to_string/1)

    %{
      coordinator: to_string(node()),
      connected_count: length(connected_peers),
      replica_count: length(replicas),
      replicas: replicas
    }
  end

  defp maybe_autofill_form(
         socket,
         shards,
         current_replicas,
         current_cluster_size,
         data_root,
         quick,
         scenarios,
         refreshed_replicas,
         refreshed_cluster_size
       ) do
    if current_replicas == socket.assigns.last_autofill_replicas and
         current_cluster_size == socket.assigns.last_autofill_cluster_size do
      socket
      |> assign(
        :form,
        to_form(
          %{
            "shards" => shards,
            "replicas" => refreshed_replicas,
            "cluster_size" => refreshed_cluster_size,
            "data_root" => data_root,
            "scenarios" => scenarios,
            "quick" => if(quick, do: "true", else: "false")
          },
          as: :bench
        )
      )
      |> assign(:last_autofill_replicas, refreshed_replicas)
      |> assign(:last_autofill_cluster_size, refreshed_cluster_size)
    else
      socket
    end
  end

  defp form_value(form, field, default) do
    case form[field].value do
      nil -> default
      value -> to_string(value)
    end
  end

  defp form_list_value(form, field, default) do
    case form[field].value do
      nil -> default
      value when is_list(value) -> Enum.map(value, &to_string/1)
      value -> [to_string(value)]
    end
  end

  defp checkbox_checked?(form, field), do: truthy?(form[field].value)

  defp scenario_checked?(form, scenario_id) do
    scenario_id in form_list_value(form, :scenarios, @default_scenarios)
  end

  defp truthy?(value) when value in [true, "true", "on", "1", 1], do: true
  defp truthy?(_), do: false

  defp default_data_root do
    if File.dir?("/data") do
      "/data/ekv_bench"
    else
      "/tmp/ekv_bench"
    end
  end

  defp latest_phase(chunk, current_phase) do
    chunk
    |> String.split("\n")
    |> Enum.reduce(nil, fn line, phase ->
      trimmed = String.trim(line)

      if Regex.match?(~r/^\d+\.\s+.+/, trimmed) do
        trimmed
      else
        phase
      end
    end)
    |> case do
      nil -> current_phase
      phase -> phase
    end
  end
end
