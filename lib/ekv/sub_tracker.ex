defmodule EKV.SubTracker do
  @moduledoc false
  alias EKV.SubTracker

  _archdoc = ~S"""
  # EKV Subscription System — Async Dispatch Architecture

  Why this exists: subscriber fan-out naturally fits inline within the shard
  GenServers as puts/deletes occur, but overhead especially with large subscribers
  would block replication workloads. We optimize this by moving fan-out of subscription
  dispatch off the write path entirely. The shard does at most one `send/2` to a companion
  dispatcher process. The dispatcher does the actual prefix matching and fan-out asynchronously.
  We pay the price of an extra message copy to the dispatcher, but free up the hot path.

  ## Components

      EKV.SubTracker (1 per EKV instance)
        Registry listener + process monitor. Maintains atomics sub_count.

      EKV.SubDispatcher (1 per shard)
        Async fan-out with batching and prefix-decomposition lookup.

      :atomics ref (1 cell, stored in config as :sub_count)
        Cell 1: total subscriber count (incremented/decremented accurately)


  ## Subscription Matching Semantics

  Subscriptions match at "/" boundaries

      ""           → matches ALL keys (wildcard)
      "user/"      → matches "user/1", "user/abc/xyz", etc.
      "user/1"     → matches exactly "user/1" only (no trailing / = exact key)

  A subscription to "foo" does NOT match "foobar". It matches only the
  literal key "foo". To match all keys under a namespace, use a trailing
  slash: "foo/" matches "foo/bar", "foo/baz/qux", etc.


  ## Data Flow: Write → Event Delivery

      Caller             Replica (shard)         SubDispatcher           Subscribers
        │                     │                        │                     │
        │ GenServer.call      │                        │                     │
        │  {:put, key, ...}   │                        │                     │
        │────────────────────>│                        │                     │
        │                     │                        │                     │
        │               SQLite write                   │                     │
        │               broadcast to peers             │                     │
        │                     │                        │                     │
        │                     │ send {:dispatch, evts} │                     │
        │                     │──────────────────────> │                     │
        │                     │                        │                     │
        │<────────────────────│  :ok returned          │                     │
        │                     │                        │                     │
        │               (caller unblocked)             │                     │
        │                     │                   drain mailbox              │
        │                     │                   (batch pending             │
        │                     │                    dispatches)               │
        │                     │                       │                      │
        │                     │                   prefix decomposition       │
        │                     │                   + Registry.lookup per      │
        │                     │                     "/" boundary             │
        │                     │                       │                      │
        │                     │                       │ send {:ekv, ...}     │
        │                     │                       │─────────────────────>│
        │                     │                       │  (to each matching   │
        │                     │                       │   subscriber pid)    │

  The shard's write path touches at most:
    - 1x :atomics.get (has_subscribers? check — ~0 cost)
    - 1x send to dispatcher (by registered name)

  For deletes: read_previous_value is gated on has_subscribers? to skip
  the ~3µs NIF call when nobody is listening.


  ## Prefix Decomposition Lookup

  Instead of scanning all subscriptions (Registry.select) and checking
  each prefix against the event key, we decompose the key into its
  possible matching prefixes and do direct Registry.lookup for each.

  For key "user/123/data", we look up exactly:
    1. ""               (match-all wildcard)
    2. "user/"          (first "/" boundary)
    3. "user/123/"      (second "/" boundary)
    4. "user/123/data"  (exact key match)

  Each Registry.lookup is a direct ETS hash lookup — O(1) per call.
  Total cost is O(slash_count + 2) regardless of subscriber count.

  ## Atomics for subscriber count

  Single cell in config.sub_count:

      Incremented by: subscribe caller (+1, immediate)
      Decremented by: SubTracker (on unregister: -1, on DOWN: -remaining)
      Read by:        Replica.has_subscribers? to gate dispatch + prev_value reads

  Why subscribe bumps atomics directly (not via SubTracker):

      The caller does :atomics.add immediately after Registry.register
      succeeds. This guarantees that by the time subscribe/2 returns,
      has_subscribers? will see the new subscriber. If we deferred to
      SubTracker, there'd be a window where the subscriber is registered
      but the shard doesn't know — writes in that window would skip
      dispatch and the subscriber would miss events.

  Why unsubscribe does NOT touch atomics directly:

      Registry.unregister triggers a {:unregister, ...} listener event
      to SubTracker, which decrements the count. This creates a brief
      window where the count is slightly high (subscriber gone from
      Registry but atomics not yet decremented). This is harmless:
      the dispatcher reads from Registry on each dispatch, so it won't
      find the unregistered subscriber regardless of the atomics count.

      The alternative (decrementing in unsubscribe) risks undercounting:
      if unsubscribe decrements but SubTracker hasn't processed the
      unregister yet, a concurrent subscribe could see count=0 and the
      shard would skip dispatch despite having active subscribers.

  Conservative invariant: count may briefly over-count, never under-counts.


  ## SubTracker: Listener + Monitor

  Registered as :"#{name}_ekv_sub_tracker", configured as a Registry
  :listener. Registry sends {:register, ...} and {:unregister, ...}
  messages for explicit register/unregister calls. In Registry, process DOWN does
  NOT send {:unregister, ...} — Registry cleans up ETS internally without
  notifying listeners. So SubTracker must monitor subscriber pids.

  State: monitors :: %{pid => {monitor_ref, count}}

      Event                Action
      ─────                ──────
      {:register, pid}     Monitor pid if new, increment pid's count.
                           Don't touch atomics (subscribe already bumped).

      {:unregister, pid}   Decrement pid's count. Demonitor if count hits 0.
                           Decrement atomics by 1.

      {:DOWN, pid}         Get remaining count from monitors map.
                           Decrement atomics by remaining count.
                           Remove pid from monitors.

  The count tracks how many prefixes a pid has subscribed to. A process
  subscribing to "user/" and "post/" has count=2. If it dies, both
  subscriptions are cleaned up in one shot (atomics decremented by 2).

  SubTracker only processes async messages from Registry (listener events)
  and {:DOWN} from the VM — no calls, no casts, no user-facing traffic routes through it.
  The hot path (writes) reads atomics directly and sends to the per-shard dispatcher.

  ## SubDispatcher: async fan-out with batching

  One per shard, registered as :"#{name}_ekv_sub_dispatcher_#{shard_index}".

  On each dispatch:
    1. Drain all pending {:dispatch, ...} from mailbox (non-blocking)
    2. For each event, decompose key at "/" boundaries
    3. Registry.lookup for each prefix candidate — O(1) per lookup
    4. Collect per-pid event lists (MapSet dedup across overlapping prefixes)
    5. Send one {:ekv, events, meta} message per subscriber pid

  Batching under load:

      When the dispatcher processes {:dispatch, events}, it first
      drains ALL pending {:dispatch, ...} messages from its mailbox
      (non-blocking receive with `after 0`). All events are merged
      into a single list, then one fan-out pass runs.

      Under sustained write load (e.g. bulk sync delivering hundreds
      of events), this collapses N dispatches into 1 fan-out pass.
      Each subscriber gets one message with all matching events instead
      of N separate messages.

  Deduplication:

      A process subscribed to both "" and "user/" receives each event
      once, not twice. The dispatcher collects matching pids into a
      MapSet per event, then builds per-pid event lists. Each event
      appears at most once per subscriber.


  ## Supervision

      EKV.Supervisor (rest_for_one)
      ├── EKV.SubTracker                          ← must exist before Registry
      ├── Registry (listeners: [sub_tracker])     ← sends events to SubTracker
      ├── EKV.SubDispatcher.Supervisor (one_for_one)
      │   ├── EKV.SubDispatcher 0
      │   ├── EKV.SubDispatcher 1
      │   └── ...
      ├── EKV.Replica.Supervisor (one_for_one)
      │   └── ...
      └── EKV.GC

  Ordering matters:
    - SubTracker before Registry: must be alive to receive listener events
      from the moment Registry starts accepting registrations.
    - Dispatchers before Replicas: must be ready when shards start writing.
    - rest_for_one: SubTracker crash restarts everything downstream.
      Registry crash restarts Dispatchers + Replicas. Dispatcher.Supervisor
      crash restarts Replicas.

  Single dispatcher crash: one_for_one restarts just that shard's
  dispatcher. The new process re-registers under the same name.
  Events dispatched to the old (dead) pid between crash and restart
  are lost — acceptable since subscribe is documented as best-effort
  (no delivery guarantee).


  ## Failure Modes

  SubTracker crash:
    rest_for_one restarts Registry + Dispatchers + Replicas. All
    subscriptions are lost. Subscribers must re-subscribe. Atomics
    are recreated (new ref in config via persistent_term).

  SubDispatcher crash:
    Restarted by one_for_one supervisor. Re-registers under the same
    name. Events between crash and restart are lost.

  Subscriber pid death:
    SubTracker receives {:DOWN}, decrements atomics by remaining
    subscription count. Between death and next dispatcher fan-out,
    dispatcher may send to the dead pid — harmless no-op in Erlang.

  Slow subscriber (full mailbox):
    send/2 never blocks. Events accumulate in the subscriber's
    mailbox. The dispatcher is not affected. This is by design —
    subscribers are responsible for draining their mailbox.
  """

  use GenServer

  defstruct [:sub_count, monitors: %{}]

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :sub_count])
    name = Keyword.fetch!(opts, :name)
    sub_count = Keyword.fetch!(opts, :sub_count)
    GenServer.start_link(__MODULE__, sub_count, name: name)
  end

  @impl true
  def init(sub_count) do
    {:ok, %SubTracker{sub_count: sub_count}}
  end

  @impl true
  def handle_info({:register, _registry, _key, pid, _value}, %SubTracker{} = state) do
    case state.monitors do
      %{^pid => {ref, count}} ->
        {:noreply, %{state | monitors: Map.put(state.monitors, pid, {ref, count + 1})}}

      %{} ->
        ref = Process.monitor(pid)
        {:noreply, %{state | monitors: Map.put(state.monitors, pid, {ref, 1})}}
    end
  end

  def handle_info({:unregister, _registry, _key, pid}, %SubTracker{} = state) do
    case state.monitors do
      %{^pid => {ref, count}} when count <= 1 ->
        Process.demonitor(ref, [:flush])
        :atomics.sub(state.sub_count, 1, 1)
        {:noreply, %{state | monitors: Map.delete(state.monitors, pid)}}

      %{^pid => {ref, count}} ->
        :atomics.sub(state.sub_count, 1, 1)
        {:noreply, %{state | monitors: Map.put(state.monitors, pid, {ref, count - 1})}}

      %{} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, %SubTracker{} = state) do
    case Map.pop(state.monitors, pid) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {{_ref, count}, monitors} ->
        :atomics.sub(state.sub_count, 1, count)
        {:noreply, %{state | monitors: monitors}}
    end
  end

  def handle_info(_msg, %SubTracker{} = state) do
    {:noreply, state}
  end
end
