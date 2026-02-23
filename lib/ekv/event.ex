defmodule EKV.Event do
  @moduledoc """
  Represents a key change event delivered to subscribers.

  - `:put` — `value` is the new value being written
  - `:delete` — `value` is the previous value before deletion (or `nil` if the key didn't exist locally)
  """

  defstruct [:type, :key, :value]

  @type t :: %__MODULE__{type: :put | :delete, key: String.t(), value: term() | nil}
end
