defmodule EKV.Supervisor do
  @moduledoc false
  use Supervisor

  _archdoc = ~S"""
  Top-level supervisor. Builds config map and stores it in `persistent_term`.

  ## Blue-Green Deployment

  When `blue_green: true`, the supervisor resolves the effective `data_dir`
  to a slot subdirectory before building config or starting children. All
  downstream modules (Replica, GC, read connections) see the resolved
  slot-specific path transparently.

  ### Slot Layout

      data_dir/
      ├── current           # marker file: "a\tnode_name@host\n"
      ├── slot_a/
      │   ├── shard_0.db
      │   ├── shard_1.db
      │   └── ...
      └── slot_b/
          ├── shard_0.db
          ├── shard_1.db
          └── ...

  ### Marker File

  The `current` marker is a single line: `"slot\tnode_name\n"` (tab-separated).
  Written atomically via write-to-tmp + `File.rename!/2` (POSIX rename is
  atomic on the same filesystem). Read on every startup to decide which slot
  to use.

  ### Slot Resolution (`resolve_blue_green_slot/2`)

  1. **No marker** → first boot. Create `slot_a/`, write marker, return
     `slot_a` path.

  2. **Marker node == `node()`** → same-VM restart (supervisor restart,
     application reload). Reopen the same slot. No snapshot needed.

  3. **Marker node != `node()`** → blue-green deploy. Snapshot current slot
     to the other slot, flip marker, return new slot path.

  ### Snapshot (`snapshot_shards/3`)

  Uses `Task.async_stream` to backup all shards in parallel. For each shard:

  1. Remove any stale WAL/SHM files at the destination (leftover from a
     previous slot occupant).
  2. Remove the destination `.db` file.
  3. Call `Store.backup_shard/3` which invokes the `ekv_backup` NIF.

  The NIF uses SQLite's online backup API (`sqlite3_backup_init/step/finish`).
  The source is opened `SQLITE_OPEN_READONLY` — safe to run while the old
  VM's WAL writer is active. `sqlite3_backup_step(backup, -1)` copies all
  pages in a single pass.

  ### Interaction with Other Features

  - **Stale DB detection**: The snapshotted database inherits the source's
    `last_active_at` timestamp. A fresh source produces a non-stale copy;
    a genuinely stale source is correctly wiped by `Store.open/3`.

  - **HWMs preserved**: The snapshot includes `kv_peer_hwm` rows, so the
    new VM can delta-sync from peers instead of requiring full sync.

  - **Oplog preserved**: The snapshot includes the full oplog, so the new
    VM advertises a meaningful `max_seq` in its peer handshake.

  - **Shard count**: Validated by `Store.open/3` as usual. The snapshot
    preserves the `kv_meta` `num_shards` row, so mismatches are caught.
  """

  @valid_opts [:name, :data_dir, :shards, :log, :tombstone_ttl, :gc_interval, :blue_green]

  def start_link(opts) do
    opts = Keyword.validate!(opts, @valid_opts)
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_ekv_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    data_dir = Keyword.fetch!(opts, :data_dir)
    num_shards = Keyword.get(opts, :shards, 8)
    blue_green = Keyword.get(opts, :blue_green, false)
    log = Keyword.get(opts, :log, :info)
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, :timer.hours(24 * 7))
    gc_interval = Keyword.get(opts, :gc_interval, :timer.minutes(5))

    data_dir =
      if blue_green,
        do: resolve_blue_green_slot(data_dir, num_shards),
        else: data_dir

    registry_name = :"#{name}_ekv_registry"
    sub_tracker_name = :"#{name}_ekv_sub_tracker"
    sub_count = :atomics.new(1, signed: true)

    config = %{
      num_shards: num_shards,
      data_dir: data_dir,
      log: log,
      tombstone_ttl: tombstone_ttl,
      gc_interval: gc_interval,
      registry: registry_name,
      sub_count: sub_count
    }

    :persistent_term.put({EKV, name}, config)

    children = [
      {EKV.SubTracker, name: sub_tracker_name, sub_count: sub_count},
      {Registry, keys: :duplicate, name: registry_name, listeners: [sub_tracker_name]},
      {EKV.SubDispatcher.Supervisor, name: name, num_shards: num_shards},
      {EKV.Replica.Supervisor, name: name, num_shards: num_shards, data_dir: data_dir},
      {EKV.GC, name: name}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  # =====================================================================
  # Blue-green slot management
  # =====================================================================

  defp resolve_blue_green_slot(data_dir, num_shards) do
    File.mkdir_p!(data_dir)

    case read_marker(data_dir) do
      nil ->
        # First boot — use slot_a
        slot_dir = Path.join(data_dir, "slot_a")
        File.mkdir_p!(slot_dir)
        write_marker(data_dir, "a", node())
        slot_dir

      {slot, node_str} ->
        if node_str == Atom.to_string(node()) do
          # Same node restart — reopen same slot
          Path.join(data_dir, "slot_#{slot}")
        else
          # Blue-green deploy — snapshot current slot to other slot
          other = if slot == "a", do: "b", else: "a"
          source_dir = Path.join(data_dir, "slot_#{slot}")
          dest_dir = Path.join(data_dir, "slot_#{other}")
          snapshot_shards(source_dir, dest_dir, num_shards)
          write_marker(data_dir, other, node())
          dest_dir
        end
    end
  end

  defp read_marker(data_dir) do
    marker_path = Path.join(data_dir, "current")

    case File.read(marker_path) do
      {:ok, contents} ->
        case String.split(String.trim(contents), "\t", parts: 2) do
          [slot, node_str] -> {slot, node_str}
          _ -> nil
        end

      {:error, _} ->
        nil
    end
  end

  defp write_marker(data_dir, slot, node_name) do
    marker_path = Path.join(data_dir, "current")
    tmp_path = marker_path <> ".tmp"
    File.write!(tmp_path, "#{slot}\t#{node_name}\n")
    File.rename!(tmp_path, marker_path)
  end

  defp snapshot_shards(source_dir, dest_dir, num_shards) do
    File.mkdir_p!(dest_dir)

    0..(num_shards - 1)
    |> Task.async_stream(
      fn shard_index ->
        dest_db = Path.join(dest_dir, "shard_#{shard_index}.db")
        File.rm(dest_db <> "-wal")
        File.rm(dest_db <> "-shm")
        File.rm(dest_db)
        :ok = EKV.Store.backup_shard(source_dir, dest_dir, shard_index)
      end,
      ordered: false
    )
    |> Stream.run()
  end
end
