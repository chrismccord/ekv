defmodule EKV.AcceptorModelPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias EKV.PropertyHelpers, as: H
  alias EKV.Store

  @moduletag :tmp_dir
  @key "model/a"
  @now 100

  property "prepare and accept remain invisible until promote, including across reopen", %{
    tmp_dir: root
  } do
    check all(
            rounds <- list_of(round_spec(), max_length: 10),
            value <- integer(-2..2)
          ) do
      # Always cross an accepted-only restart and a successful promote.
      # Virtual scan times exercise exact metadata boundaries without sleeps.
      rounds =
        [{value, nil, false, :accept_only}, {value, 101, false, :promote}] ++ rounds

      commands =
        rounds
        |> Enum.with_index(1)
        |> Enum.flat_map(fn {{value, expires, delete?, boundary}, ballot} ->
          row =
            {:erlang.term_to_binary(value), ballot * 10, "writer", expires, if(delete?, do: @now)}

          before_accept = [{:prepare, ballot}, {:stale_accept, ballot, row}]

          case boundary do
            :prepare_only ->
              before_accept ++ [:reopen]

            :accept_only ->
              before_accept ++ [{:accept, ballot, row}, :reopen]

            :promote ->
              before_accept ++
                [
                  {:accept, ballot, row},
                  :reopen,
                  {:stale_promote, ballot},
                  {:promote, ballot},
                  :reopen
                ]
          end
        end)

      H.in_directory(root, fn directory ->
        model =
          H.sessions(
            commands,
            %{accepted: nil, ballot: 0, committed: nil, commits: 0, accepts: 0, rejected: 0},
            fn -> H.open_store(directory) end,
            &H.close_store/1,
            &step/3,
            &assert_visible/2
          )

        assert model.accepts >= 2
        assert model.commits >= 1
        assert model.rejected >= 3
      end)
    end
  end

  # Deliberately a fixed regression, not a claim of a partition-safe CAS mode
  # fence. The stale peer's eventual write may be newer than the CAS quorum saw.
  test "migration rejects local LWW but permits a newer remote LWW row on heal", %{tmp_dir: root} do
    H.in_directory(root, fn directory ->
      {db, stmts} = resource = H.open_store(directory)

      try do
        cas = :erlang.term_to_binary(:cas)
        stale_peer = :erlang.term_to_binary(:stale_peer)
        assert {:ok, :promise, 0, "", nil} = Store.paxos_prepare(db, @key, 1, "proposer")

        assert {:ok, true} =
                 Store.paxos_accept(db, @key, 1, "proposer", [cas, 10, "writer", nil, nil])

        assert {:ok, ^cas, 10, "writer", nil, nil, nil, 1, 1} = promote(resource, 1)

        assert {:error, :cas_managed_key} =
                 Store.write_entry(
                   db,
                   stmts.kv_upsert,
                   stmts.keyref_upsert,
                   stmts.oplog_insert,
                   @key,
                   stale_peer,
                   20,
                   "writer",
                   nil,
                   nil,
                   nil,
                   true,
                   true
                 )

        assert {:ok, true, 1, 1} =
                 Store.write_entry(
                   db,
                   stmts.kv_upsert,
                   stmts.keyref_upsert,
                   stmts.oplog_insert,
                   @key,
                   stale_peer,
                   20,
                   "remote",
                   nil,
                   nil,
                   1
                 )

        assert Store.get(db, @key) == {stale_peer, 20, "remote", nil, nil}
      after
        H.close_store(resource)
      end
    end)
  end

  defp round_spec do
    tuple(
      {integer(-2..2), member_of([nil, @now - 1, @now, @now + 1]), boolean(),
       member_of([:prepare_only, :accept_only, :promote])}
    )
  end

  defp step({db, _stmts}, {:prepare, ballot}, model) do
    accepted_node = if model.ballot == 0, do: "", else: "proposer"
    row = if model.accepted, do: Tuple.to_list(model.accepted)

    assert Store.paxos_prepare(db, @key, ballot, "proposer") ==
             {:ok, :promise, model.ballot, accepted_node, row}

    assert Store.paxos_prepare(db, @key, ballot, "proposer") ==
             {:ok, :nack, ballot, "proposer"}

    model
  end

  defp step({db, _stmts}, {:stale_accept, ballot, row}, model) do
    assert {:ok, false} =
             Store.paxos_accept(db, @key, ballot - 1, "proposer", Tuple.to_list(row))

    %{model | rejected: model.rejected + 1}
  end

  defp step({db, _stmts}, {:accept, ballot, row}, model) do
    assert {:ok, true} = Store.paxos_accept(db, @key, ballot, "proposer", Tuple.to_list(row))
    %{model | accepted: row, ballot: ballot, accepts: model.accepts + 1}
  end

  defp step(resource, {:stale_promote, ballot}, model) do
    assert {:ok, :stale} = promote(resource, ballot - 1)
    %{model | rejected: model.rejected + 1}
  end

  defp step(resource, {:promote, ballot}, model) do
    {value, ts, origin, expires, deleted} = model.accepted

    assert {:ok, ^value, ^ts, ^origin, ^expires, ^deleted, _old, seq, progress} =
             promote(resource, ballot)

    assert seq == model.commits + 1
    assert progress == seq
    %{model | committed: model.accepted, commits: model.commits + 1}
  end

  defp promote({db, stmts}, ballot) do
    Store.paxos_promote(
      db,
      stmts.kv_force_upsert,
      stmts.keyref_upsert,
      stmts.oplog_insert,
      @key,
      ballot,
      "proposer"
    )
  end

  defp assert_visible({db, _stmts}, model) do
    assert Store.get(db, @key) == model.committed

    live =
      case model.committed do
        {value, _ts, _origin, expires, deleted}
        when (is_nil(expires) or expires > @now) and (is_nil(deleted) or deleted > @now) ->
          [{@key, value}]

        _ ->
          []
      end

    assert Store.scan_prefix(db, "model/", @now) == live
    assert Store.scan_prefix_keys(db, "model/", @now) == Enum.map(live, &elem(&1, 0))
    assert length(Store.oplog_since(db, 0)) == model.commits
  end
end
