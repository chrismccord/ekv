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
    {"9", "9. CAS vs LWW Put Comparison"},
    {"10", "10. Large Payload Put Latency"}
  ]

  @default_scenarios ~w(4 5 6 7 8 9)
  @role_options [
    {"Member", "member"},
    {"Client", "client"},
    {"Ignore", "ignore"}
  ]

  @wan_9_member_counts %{
    "iad" => 2,
    "dfw" => 2,
    "ewr" => 1,
    "sjc" => 2,
    "lhr" => 1,
    "fra" => 1
  }

  @region_preferences %{
    "iad" => ~w(iad ewr dfw sjc yyz ord lhr ams fra cdg),
    "ewr" => ~w(ewr iad dfw sjc yyz ord lhr ams fra cdg),
    "dfw" => ~w(dfw iad ewr sjc ord lax sea lhr fra ams),
    "sjc" => ~w(sjc dfw iad ewr lax sea ord lhr fra ams),
    "lhr" => ~w(lhr fra ams cdg iad ewr dfw sjc),
    "fra" => ~w(fra ams lhr cdg ewr iad dfw sjc),
    "ams" => ~w(ams fra lhr cdg ewr iad dfw sjc),
    "cdg" => ~w(cdg lhr fra ams ewr iad dfw sjc)
  }

  @impl true
  def mount(_params, _session, socket) do
    summary = cluster_summary()
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
      |> assign(:role_options, @role_options)
      |> assign(:summary, summary)
      |> assign(:form, default_form(summary, data_root, scenarios, quick))

    if connected?(socket) do
      Process.send_after(self(), :refresh_cluster, 1_000)
    end

    {:ok, socket}
  end

  @impl true
  def handle_event("validate", %{"bench" => params}, socket) do
    summary = cluster_summary()
    params = ensure_form_params(params, summary.nodes)

    {:noreply,
     socket
     |> assign(:summary, summary)
     |> assign(:form, to_form(params, as: :bench))}
  end

  @impl true
  def handle_event("preset_wan_9", _params, socket) do
    summary = cluster_summary()
    params = current_form_params(socket.assigns.form)
    params = ensure_form_params(params, summary.nodes)
    params = Map.put(params, "roles", wan_9_role_map(summary.nodes))

    {:noreply,
     socket
     |> assign(:summary, summary)
     |> assign(:form, to_form(params, as: :bench))}
  end

  @impl true
  def handle_event("run", %{"bench" => _params}, %{assigns: %{running: true}} = socket) do
    {:noreply, put_flash(socket, :error, "A benchmark run is already in progress")}
  end

  @impl true
  def handle_event("run", %{"bench" => params}, socket) do
    summary = cluster_summary()
    params = ensure_form_params(params, summary.nodes)

    with {:ok, shards} <- parse_shards(params["shards"]),
         {:ok, {members, clients}} <- parse_topology(summary.nodes, params["roles"]),
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
              members: members,
              clients: clients,
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
       |> assign(:summary, summary)
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
         socket
         |> assign(:summary, summary)
         |> assign(:form, to_form(params, as: :bench))
         |> put_flash(:error, reason)}
    end
  end

  @impl true
  def handle_info(:refresh_cluster, socket) do
    summary = cluster_summary()
    params = ensure_form_params(current_form_params(socket.assigns.form), summary.nodes)

    socket =
      socket
      |> assign(:summary, summary)
      |> assign(:form, to_form(params, as: :bench))

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
    counts = topology_counts(assigns.form, assigns.summary.nodes)

    assigns =
      assign(assigns,
        selected_member_count: counts.member_count,
        selected_client_count: counts.client_count,
        selected_ignore_count: counts.ignore_count
      )

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
                Start EKV members and clients on demand across connected Fly machines.
              </p>
            </div>
            <div class="rounded-xl border border-zinc-800 bg-zinc-900 px-4 py-3 text-xs text-zinc-300">
              <p>coordinator: <span class="text-zinc-100">{@summary.coordinator}</span></p>
              <p class="mt-1">
                connected nodes: <span class="text-zinc-100">{@summary.connected_count}</span>
              </p>
              <p class="mt-1">
                discovered regions:
                <span class="text-zinc-100">{format_region_counts(@summary.region_counts)}</span>
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

            <div class="grid gap-2 rounded-lg border border-zinc-800 bg-zinc-900/40 p-3">
              <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <p class="text-xs font-medium uppercase tracking-[0.18em] text-zinc-400">
                    Node Topology
                  </p>
                  <p class="mt-1 text-xs text-zinc-500">
                    Members run durable EKV replicas. Clients route to members using region-aware defaults.
                  </p>
                </div>
                <button
                  type="button"
                  phx-click="preset_wan_9"
                  class="rounded-lg border border-cyan-800/60 bg-cyan-950/40 px-3 py-2 text-xs font-medium text-cyan-200 transition hover:border-cyan-700 hover:bg-cyan-900/40"
                >
                  Apply 9-member WAN preset
                </button>
              </div>

              <div class="grid gap-2">
                <%= for node <- @summary.nodes do %>
                  <div class="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-zinc-800 bg-zinc-950/60 px-3 py-3">
                    <div class="min-w-0 flex-1">
                      <p class="truncate font-mono text-xs text-zinc-100">{node.name}</p>
                      <p class="mt-1 text-[11px] uppercase tracking-[0.18em] text-cyan-300/80">
                        {node.region}
                      </p>
                      <%= if role_value(@form, node.name) == "client" do %>
                        <p class="mt-1 text-[11px] text-zinc-500">
                          routes: {Enum.join(
                            route_preview(@form, @summary.nodes, node.region),
                            " -> "
                          )}
                        </p>
                      <% end %>
                    </div>

                    <label class="grid gap-1 text-xs text-zinc-400">
                      <span>Role</span>
                      <select
                        name={"bench[roles][#{node.name}]"}
                        class="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none ring-cyan-500 transition focus:ring-2"
                      >
                        <%= for {label, value} <- @role_options do %>
                          <option value={value} selected={role_value(@form, node.name) == value}>
                            {label}
                          </option>
                        <% end %>
                      </select>
                    </label>
                  </div>
                <% end %>
              </div>

              <p class="text-xs text-zinc-500">
                Selected: {@selected_member_count} members, {@selected_client_count} clients, {@selected_ignore_count} ignored.
                Member quorum = {quorum_size(@selected_member_count)} when at least 3 members are selected.
              </p>
            </div>

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
                /> Quick mode (lower iteration counts for faster feedback)
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
                Members start with `wait_for_quorum`. Clients start with `wait_for_route` + `wait_for_quorum`.
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
              Discovered nodes: {Enum.map_join(@summary.nodes, ", ", & &1.name)}
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

  defp default_form(summary, data_root, scenarios, quick) do
    to_form(
      ensure_form_params(
        %{
          "shards" => "8",
          "data_root" => data_root,
          "scenarios" => scenarios,
          "quick" => if(quick, do: "true", else: "false")
        },
        summary.nodes
      ),
      as: :bench
    )
  end

  defp current_form_params(form) do
    form.params || %{}
  end

  defp ensure_form_params(params, nodes) do
    params
    |> Map.new(fn {key, value} -> {to_string(key), value} end)
    |> Map.put_new("shards", "8")
    |> Map.put_new("data_root", default_data_root())
    |> Map.put_new("scenarios", @default_scenarios)
    |> Map.put_new("quick", "true")
    |> Map.update("roles", default_role_map(nodes), &ensure_role_map(&1, nodes))
  end

  defp ensure_role_map(raw_roles, nodes) when is_map(raw_roles) do
    defaults = default_role_map(nodes)

    Enum.reduce(nodes, defaults, fn node, acc ->
      Map.put(acc, node.name, normalize_role(Map.get(raw_roles, node.name)))
    end)
  end

  defp ensure_role_map(_raw_roles, nodes), do: default_role_map(nodes)

  defp default_role_map(nodes) do
    Map.new(nodes, fn node -> {node.name, "member"} end)
  end

  defp wan_9_role_map(nodes) do
    selected_names =
      nodes
      |> Enum.group_by(& &1.region)
      |> Enum.flat_map(fn {region, region_nodes} ->
        count = Map.get(@wan_9_member_counts, region, 0)

        region_nodes
        |> Enum.sort_by(& &1.name)
        |> Enum.take(count)
        |> Enum.map(& &1.name)
      end)
      |> MapSet.new()

    Map.new(nodes, fn node ->
      role = if MapSet.member?(selected_names, node.name), do: "member", else: "client"
      {node.name, role}
    end)
  end

  defp parse_shards(nil), do: {:error, "shards is required"}

  defp parse_shards(value) do
    case Integer.parse(to_string(value)) do
      {n, ""} when n > 0 -> {:ok, n}
      _ -> {:error, "shards must be a positive integer"}
    end
  end

  defp parse_topology(nodes, raw_roles) do
    roles = ensure_role_map(raw_roles || %{}, nodes)

    members =
      nodes
      |> Enum.filter(&(Map.get(roles, &1.name) == "member"))
      |> Enum.map(fn node -> %{node: node.atom, region: node.region} end)

    if length(members) < 3 do
      {:error, "select at least 3 member nodes"}
    else
      member_regions = members |> Enum.map(& &1.region) |> Enum.uniq()

      clients =
        nodes
        |> Enum.filter(&(Map.get(roles, &1.name) == "client"))
        |> Enum.map(fn node ->
          %{
            node: node.atom,
            region: node.region,
            region_routing: default_region_routing(node.region, member_regions)
          }
        end)

      {:ok, {members, clients}}
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
          {n, ""} when n in 1..10 -> {:ok, n}
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
    nodes =
      [node() | Node.list()]
      |> Enum.uniq()
      |> Enum.map(&fetch_node_metadata/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.sort_by(fn node -> {node.region, node.name} end)

    %{
      coordinator: to_string(node()),
      connected_count: max(length(nodes) - 1, 0),
      region_counts: Enum.frequencies_by(nodes, & &1.region),
      nodes: nodes
    }
  end

  defp fetch_node_metadata(remote_node) when remote_node == node() do
    metadata = Bench.Replica.node_metadata()
    %{name: metadata.name, atom: metadata.node, region: metadata.region}
  end

  defp fetch_node_metadata(remote_node) do
    try do
      metadata = :erpc.call(remote_node, Bench.Replica, :node_metadata, [], 1_000)
      %{name: metadata.name, atom: metadata.node, region: metadata.region}
    catch
      _, _ -> nil
    end
  end

  defp role_value(form, node_name) do
    form
    |> current_form_params()
    |> Map.get("roles", %{})
    |> Map.get(node_name, "member")
    |> normalize_role()
  end

  defp route_preview(form, nodes, client_region) do
    roles = current_form_params(form) |> Map.get("roles", %{}) |> ensure_role_map(nodes)

    member_regions =
      nodes
      |> Enum.filter(&(Map.get(roles, &1.name) == "member"))
      |> Enum.map(& &1.region)
      |> Enum.uniq()

    default_region_routing(client_region, member_regions)
  end

  defp topology_counts(form, nodes) do
    roles = current_form_params(form) |> Map.get("roles", %{}) |> ensure_role_map(nodes)

    Enum.reduce(nodes, %{member_count: 0, client_count: 0, ignore_count: 0}, fn node, acc ->
      case Map.get(roles, node.name, "member") do
        "member" -> %{acc | member_count: acc.member_count + 1}
        "client" -> %{acc | client_count: acc.client_count + 1}
        "ignore" -> %{acc | ignore_count: acc.ignore_count + 1}
      end
    end)
  end

  defp normalize_role(role) when role in ["member", "client", "ignore"], do: role
  defp normalize_role(_role), do: "member"

  defp format_region_counts(region_counts) when map_size(region_counts) == 0, do: "none"

  defp format_region_counts(region_counts) do
    region_counts
    |> Enum.sort_by(fn {region, _count} -> region end)
    |> Enum.map_join(", ", fn {region, count} -> "#{region}(#{count})" end)
  end

  defp quorum_size(member_count) when member_count >= 3, do: div(member_count, 2) + 1
  defp quorum_size(_member_count), do: 0

  defp default_region_routing(client_region, member_regions) do
    preferences = Map.get(@region_preferences, client_region, [client_region])

    member_regions
    |> Enum.uniq()
    |> Enum.sort_by(fn region -> {preference_index(preferences, region), region} end)
  end

  defp preference_index(preferences, region) do
    Enum.find_index(preferences, &(&1 == region)) || length(preferences) + 1
  end

  defp checkbox_checked?(form, field), do: truthy?(form[field].value)

  defp scenario_checked?(form, scenario_id) do
    scenario_id in form_list_value(form, :scenarios, @default_scenarios)
  end

  defp form_list_value(form, field, default) do
    case form[field].value do
      nil -> default
      value when is_list(value) -> Enum.map(value, &to_string/1)
      value -> [to_string(value)]
    end
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
