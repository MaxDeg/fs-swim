module SwimProtocol.Message

open MsgPack
open System.Net

let private encodeEvents events = ()

let private encodeMember memb = 
    Array [|
        String memb.Name
        String(memb.Address.Address.ToString())
        Int32 memb.Address.Port
    |]

let encodePing seqNr = 
    UInt64 seqNr |> Packer.pack

let encodePingRequest seqNr memb = 
    Packer.packArray [|
        yield UInt64 seqNr
        yield encodeMember memb
    |]

let encodeAck seqNr memb = 
     Packer.packArray [|
        yield UInt64 seqNr
        yield encodeMember memb
    |]

let private decodeEvents events = ()

let private (|Member|_|) values =
    match values with
    | Array [| String name; String ip; Int32 port |] ->
        Some { Name = name
               Address = new IPEndPoint(IPAddress.Parse(ip), int port) }
    | _ -> None

let decodePing bytes =
    match Unpacker.unpack bytes with
    | UInt64 seqNr -> Ping seqNr |> Some
    | _ -> None

let decodePingRequest bytes =
    match Unpacker.unpack bytes with
    | Array [| UInt64 seqNr; Member memb |] -> 
        PingRequest(seqNr, memb) |> Some 
    | _ -> None

let decodeAck bytes =
    match Unpacker.unpack bytes with
    | Array [| UInt64 seqNr; Member memb |] -> 
        Ack(seqNr, memb) |> Some 
    | _ -> None

let encode msg : byte[] =
    match msg with
    | Ping s -> Extension(0y, encodePing s)
    | PingRequest(s, m) -> Extension(1y, encodePingRequest s m)
    | Ack(s, m) -> Extension(2y, encodeAck s m)
    |> Packer.pack

let decode bytes =
    match Unpacker.unpack bytes with
    | Value.Extension(id, rest) when id = 0y -> decodePing rest
    | Value.Extension(id, rest) when id = 1y -> decodePingRequest rest
    | Value.Extension(id, rest) when id = 2y -> decodeAck rest
    | _ -> None
