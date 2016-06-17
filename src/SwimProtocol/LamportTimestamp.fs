module LamportTimestamp

(*
    Implentation of Lamport Timestamp
*)

open System

type Timestamp = Timestamp of int64

let increase (Timestamp local) =
    local + 1L |> Timestamp

let merge (Timestamp local) (Timestamp remote) =
    Math.Max(local, remote) + 1L |> Timestamp