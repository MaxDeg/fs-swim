module SwimProtocol.Transport

open System
open System.IO
open System.Net
open System.Net.Sockets
open FSharp.Control.Reactive.Builders
open MsgPack
open FSharpx.Control

type TransportMessage = IPEndPoint * byte []

type Socket = 
    private
    | Socket of UdpClient * IObservable<TransportMessage>

let private listener (udp : UdpClient) callback = 
    async { 
        while true do
            let! msg = udp.ReceiveAsync() |> Async.AwaitTask
            callback (msg.RemoteEndPoint, msg.Buffer)
    }

let create port = 
    let udp = new UdpClient(port = port)    
    let subject = new Subject<TransportMessage>()
    subject.OnNext |> listener udp |> Async.Start

    Socket(udp, subject)

let send (Socket(udp, _)) addr buffer =
    udp.SendAsync(buffer, buffer.Length, addr)
    |> Async.AwaitTask
    |> Async.Ignore

let receive (Socket(_, observable)) = observable

let private encodeEvents events = ()

let private encodeMember memb = 
    Array.concat [
        memb.Name |> Packer.packString
        memb.Address.Address.ToString()  |> Packer.packString
        memb.Address.Port |> Packer.packInt
    ]

let private encodePing seqNr = 
    seqNr |> Packer.packUInt64

let private encodePingRequest seqNr memb =
    Array.concat [
        seqNr |> Packer.packUInt64
        encodeMember memb
    ]

let private encodeAck seqNr memb =
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
    | Value.UInt8 i -> Some(uint64 i)
    | Value.UInt16 i -> Some(uint64 i)
    | Value.UInt32 i -> Some(uint64 i)
    | Value.UInt64 i -> Some i
    | _ -> None

let private (|Member|_|) values =
    match values with
    | [ Value.String name; Value.String ip; Value.UInt16 port ] ->
        Some { Name = name
               Address = new IPEndPoint(IPAddress.Parse(ip), int port) }
    | _ -> None

let private decodePing bytes =
    match Unpacker.unpack bytes with
    | [| UInt64 seqNr |] -> Ping seqNr |> Some
    | _ -> None

let private decodePingRequest bytes =
    match Unpacker.unpack bytes |> List.ofArray with
    | UInt64 seqNr :: Member(memb) -> 
        PingRequest(seqNr, memb) |> Some 
    | _ -> None

let private decodeAck bytes =
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
    match Unpacker.unpack bytes with
    | [| Value.Ext(id, rest) |] when id = 0y -> decodePing rest
    | [| Value.Ext(id, rest) |] when id = 1y -> decodePingRequest rest
    | [| Value.Ext(id, rest) |] when id = 2y -> decodeAck rest
    | _ -> None
