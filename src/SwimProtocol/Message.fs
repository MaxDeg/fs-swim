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
    
    let private (|Node|_|) values =
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
        | UInt64 seqNr -> Sequence.makeFrom seqNr |> Ping |> Some
        | _ -> None

    // PingRequest encoding
    let private encodePingRequest seqNr memb = 
        Packer.packArray [|
            yield UInt64 seqNr
            yield encodeNode memb
        |]
    
    let private decodePingRequest bytes =
        match Unpacker.unpack bytes with
        | Array [| UInt64 seqNr; Node memb |] -> 
            PingRequest(Sequence.makeFrom seqNr, memb) |> Some 
        | _ -> None

    // Ack encoding
    let private encodeAck seqNr memb = 
        Packer.packArray [|
            yield UInt64 seqNr
            yield encodeNode memb
        |]
    
    let private decodeAck = 
        Unpacker.unpack >> function
        | Array [| UInt64 seqNr; Node memb |] -> 
            Ack(Sequence.makeFrom seqNr, memb) |> Some 
        | _ -> None
        
    // Leave encoding
    let private encodeLeave inc =
        UInt64 inc |> Packer.pack
        
    let private decodeLeave = 
        Unpacker.unpack >> function
        | UInt64 inc -> IncarnationNumber.makeFrom inc |> Leave |> Some
        | _ -> None

    // Events encoding
    let private decodeEvents values =
        let rec decodeNextEvent acc = function
        | UInt8 id::Node m::UInt64 inc::tail when id = 0uy ->
            decodeNextEvent (Membership(m, IncarnationNumber.makeFrom inc |> Alive) :: acc) tail
            
        | UInt8 id::Node m::UInt64 inc::tail when id = 1uy ->
            decodeNextEvent (Membership(m, IncarnationNumber.makeFrom inc |> Suspect) :: acc) tail
            
        | UInt8 id::Node m::UInt64 inc::tail when id = 2uy ->
            decodeNextEvent (Membership(m, IncarnationNumber.makeFrom inc |> Dead) :: acc) tail
            
        | String e::tail ->
            decodeNextEvent(User e :: acc) tail
            
        | [] -> acc
        | _ -> []

        decodeNextEvent [] values
    
    let encodeEvent = function
        | Membership(m, Alive(IncarnationNumber.Number inc)) -> [| UInt8 0uy; encodeNode m; UInt64 inc |]
        | Membership(m, Suspect(IncarnationNumber.Number inc)) -> [| UInt8 1uy; encodeNode m; UInt64 inc |]
        | Membership(m, Dead(IncarnationNumber.Number inc)) -> [| UInt8 2uy; encodeNode m; UInt64 inc |]
        | User e -> [| String e |]
    
    let encodeMessage msg =
        match msg with
        | Ping(Sequence.Number s) -> Extension(0y, encodePing s)
        | PingRequest(Sequence.Number s, m) -> Extension(1y, encodePingRequest s m)
        | Ack(Sequence.Number s, m) -> Extension(2y, encodeAck s m)
        | Leave(IncarnationNumber.Number i) -> Extension(3y, encodeLeave i)

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
        | Arraylist(Value.Extension(id, rest) :: events) when id = 3y ->
            decodeLeave rest |> Option.map (fun p -> p, decodeEvents events)            
        | _ -> None

    let sizeOf value =
        Packer.pack value |> Array.length
        
    let sizeOfValues =
        Array.fold (fun cnt value -> (Packer.pack value |> Array.length) + cnt) 0
    