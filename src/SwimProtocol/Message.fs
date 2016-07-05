namespace SwimProtocol

open MsgPack
open System.Net

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Message =
    // Node encoding
    let private encodeNode node = 
        Array [|
            Int64 node.IPAddress
            UInt16 node.Port
        |]
    
    let private (|Member|_|) values =
        match values with
        | Array [| Int64 ip; UInt16 port |] ->
            Some { IPAddress = ip
                   Port = port }
        | _ -> None

    // Ping encoding
    let private encodePing seqNr = 
        UInt64 seqNr |> Packer.pack

    let private decodePing bytes =
        match Unpacker.unpack bytes with
        | UInt64 seqNr -> Ping seqNr |> Some
        | _ -> None

    // PingRequest encoding
    let private encodePingRequest seqNr memb = 
        Packer.packArray [|
            yield UInt64 seqNr
            yield encodeNode memb
        |]
    
    let private decodePingRequest bytes =
        match Unpacker.unpack bytes with
        | Array [| UInt64 seqNr; Member memb |] -> 
            PingRequest(seqNr, memb) |> Some 
        | _ -> None

    // Ack encoding
    let private encodeAck seqNr memb = 
        Packer.packArray [|
            yield UInt64 seqNr
            yield encodeNode memb
        |]
    
    let private decodeAck bytes =
        match Unpacker.unpack bytes with
        | Array [| UInt64 seqNr; Member memb |] -> 
            Ack(seqNr, memb) |> Some 
        | _ -> None

    // Events encoding
    let private decodeEvents values =
        let rec decodeNextEvent acc = function
        | UInt8 id::Member m::UInt64 inc::tail when id = 0uy ->
            decodeNextEvent (MembershipEvent(Alive(m, inc)) :: acc) tail
        | UInt8 id::Member m::UInt64 inc::tail when id = 1uy ->
            decodeNextEvent (MembershipEvent(Suspect(m, inc)) :: acc) tail
        | UInt8 id::Member m::UInt64 inc::tail when id = 2uy ->
            decodeNextEvent (MembershipEvent(Dead(m, inc)) :: acc) tail
        | String e::tail ->
            decodeNextEvent(UserEvent e :: acc) tail
        | [] -> acc
        | _ -> []

        decodeNextEvent [] values
    
    let encodeEvent = function
        | MembershipEvent(Alive(m, inc)) -> [| UInt8 0uy; encodeNode m; UInt64 inc |]
        | MembershipEvent(Suspect(m, inc)) -> [| UInt8 1uy; encodeNode m; UInt64 inc |]
        | MembershipEvent(Dead(m, inc)) -> [| UInt8 2uy; encodeNode m; UInt64 inc |]
        | UserEvent e -> [| String e |]
    
    let encodeMessage msg =
        match msg with
        | Ping s -> Extension(0y, encodePing s)
        | PingRequest(s, m) -> Extension(1y, encodePingRequest s m)
        | Ack(s, m) -> Extension(2y, encodeAck s m)

    let encode msg events : byte[] =
        Array [|
            yield msg
            yield! events
        |] |> Packer.pack

    let decode bytes =
        match Unpacker.unpack bytes with
        | Arraylist(Value.Extension(id, rest) :: events) when id = 0y ->
            decodePing rest |> Option.map (fun p -> p, decodeEvents events)
        | Arraylist(Value.Extension(id, rest) :: events) when id = 1y ->
            decodePingRequest rest |> Option.map (fun p -> p, decodeEvents events)
        | Arraylist(Value.Extension(id, rest) :: events) when id = 2y ->
            decodeAck rest |> Option.map (fun p -> p, decodeEvents events)
        | _ -> None

    let sizeOf value =
        Packer.pack value |> Array.length
        
    let sizeOfValues =
        Array.fold (fun cnt value -> (Packer.pack value |> Array.length) + cnt) 0
    