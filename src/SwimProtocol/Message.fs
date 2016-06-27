namespace SwimProtocol

open MsgPack
open System.Net

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Message =
    let private encodeMember memb = 
        Array [|
            String memb.Name
            String(memb.Address.Address.ToString())
            Int32 memb.Address.Port
        |]
        
    let private encodeEvents values =
        Array.map (function
                    | MembershipEvent(Alive(m, inc)) -> Extension(0y, Packer.packArray [| encodeMember m; UInt64 inc |])
                    | MembershipEvent(Suspect(m, inc)) -> Extension(1y, Packer.packArray [| encodeMember m; UInt64 inc |])
                    | MembershipEvent(Dead(m, inc)) -> Extension(2y, Packer.packArray [| encodeMember m; UInt64 inc |])
                    | UserEvent e -> String e) values
        |> Array

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

    let private (|Member|_|) values =
        match values with
        | Array [| String name; String ip; Int32 port |] ->
            Some { Name = name
                   Address = new IPEndPoint(IPAddress.Parse(ip), int port) }
        | _ -> None

    let private (|Events|_|) = function 
        | Array values ->
            let decode rest =
                match Unpacker.unpack rest with
                | Array [| Member m; UInt64 inc |] -> Some (m, inc)
                | _ -> None
            
            values
            |> Array.map(function
                        | Array [| Value.Extension(id, rest) |] when id = 0y ->
                            Option.map (fun r -> r |> Alive |> MembershipEvent) (decode rest)
                        | Array [| Value.Extension(id, rest) |] when id = 1y ->
                            Option.map (fun r -> r |> Suspect |> MembershipEvent) (decode rest)
                        | Array [| Value.Extension(id, rest) |] when id = 2y ->
                            Option.map (fun r -> r |> Dead |> MembershipEvent) (decode rest)
                        | Array [| String s |] -> Some (UserEvent s)
                        | _ -> None)
            |> Some
        | _ -> None

    let decodePing bytes =
        match Unpacker.unpack bytes with
        | UInt64 seqNr -> PingMessage seqNr |> Some
        | _ -> None

    let decodePingRequest bytes =
        match Unpacker.unpack bytes with
        | Array [| UInt64 seqNr; Member memb |] -> 
            PingRequestMessage(seqNr, memb) |> Some 
        | _ -> None

    let decodeAck bytes =
        match Unpacker.unpack bytes with
        | Array [| UInt64 seqNr; Member memb |] -> 
            AckMessage(seqNr, memb) |> Some 
        | _ -> None

    let encodeMessage msg =
        match msg with
        | PingMessage s -> Extension(0y, encodePing s)
        | PingRequestMessage(s, m) -> Extension(1y, encodePingRequest s m)
        | AckMessage(s, m) -> Extension(2y, encodeAck s m)
        |> Packer.pack

    let encode msg events : byte[] =
        printfn "Message %A - %A" msg events
        try
            match msg with
            | PingMessage s -> [| yield Extension(0y, encodePing s); yield encodeEvents events |]
            | PingRequestMessage(s, m) -> [| yield Extension(1y, encodePingRequest s m); yield encodeEvents events |]
            | AckMessage(s, m) -> [| yield Extension(2y, encodeAck s m); yield encodeEvents events |]
            |> Array
            |> Packer.pack
        with e -> printfn "Error Encoding : %A" e
                  Array.empty

    let decode bytes =
        try
            match Unpacker.unpack bytes with
            | Array [| Value.Extension(id, rest); Events events |] when id = 0y -> decodePing rest |> Option.map (fun p -> p, events)
            | Array [| Value.Extension(id, rest); Events events |] when id = 1y -> decodePingRequest rest |> Option.map (fun p -> p, events)
            | Array [| Value.Extension(id, rest); Events events |] when id = 2y -> decodeAck rest |> Option.map (fun p -> p, events)
            | _ -> None
        with e -> printfn "Error Encoding : %A" e
                  None
