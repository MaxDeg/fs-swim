module SwimProtocol.Message

//open MsgPack
open System.Net

type Ping = PeriodSeqNumber
type PingRequest = PeriodSeqNumber * Member
type Ack = PeriodSeqNumber * Member

type Message = 
    | Ping of Ping
    | PingRequest of PingRequest
    | Ack of Ack

let private encodeEvents events = ()

let private encodeMember memb = 
    Array.concat [
        memb.Name |> Packer.packString
        memb.Address.Address.ToString()  |> Packer.packString
        memb.Address.Port |> Packer.packInt
    ]

let encodePing seqNr =
    seqNr |> Packer.packUInt64

let encodePingRequest seqNr memb =
    Array.concat [
        seqNr |> Packer.packUInt64
        encodeMember memb
    ]

let encodeAck seqNr memb =
    Array.concat [
        seqNr |> Packer.packUInt64
        encodeMember memb
    ]

let private decodeEvents events = ()

let private (|Int32|_|) value =
    match value with
    | Value.Int8 i -> Some(int32 i)
    | Value.Int16 i -> Some(int32 i)
    | Value.Int32 i -> Some(i)
    | _ -> None

let private (|UInt64|_|) value =
    match value with
    | Value.UInt64 i -> Some i
    | Value.UInt32 i -> Some(uint64 i)
    | Value.UInt16 i -> Some(uint64 i)
    | Value.UInt8 i -> Some(uint64 i)
    | _ -> None

let private (|Member|_|) values =
    match values with
    | [ Value.String name; Value.String ip; Value.UInt16 port ] ->
        Some { Name = name
               Address = new IPEndPoint(IPAddress.Parse(ip), int port) }
    | _ -> None

let decodePing bytes =
    printfn "Decode Ping %A" bytes
    match Unpacker.unpack bytes with
    | [| UInt64 seqNr |] -> Ping seqNr |> Some
    | _ -> None

let decodePingRequest bytes =
    printfn "Decode PingRequest %A" bytes
    match Unpacker.unpack bytes |> List.ofArray with
    | UInt64 seqNr :: Member(memb) -> 
        PingRequest(seqNr, memb) |> Some 
    | _ -> None

let decodeAck bytes =
    printfn "Decode Ack %A" bytes
    match Unpacker.unpack bytes |> List.ofArray with
    | UInt64 seqNr :: Member(memb) -> 
        Ack(seqNr, memb) |> Some 
    | _ -> None

let encode msg : byte[] =
    match msg with
    | Ping s -> encodePing s |> Packer.packExt 0y
    | PingRequest(s, m) -> encodePingRequest s m |> Packer.packExt 1y
    | Ack(s, m) -> encodeAck s m |> Packer.packExt 2y

let decode bytes =
    try
        printfn "Decode %A" bytes
        match Unpacker.unpack bytes with
        | [| Value.Ext(id, rest) |] when id = 0y -> decodePing rest
        | [| Value.Ext(id, rest) |] when id = 1y -> decodePingRequest rest
        | [| Value.Ext(id, rest) |] when id = 2y -> decodeAck rest
        | _ -> printfn "fail decode";None
    with
    | e -> 
        printfn "Error %A" e
        reraise()
