#r "../../packages/FSharpx.Collections/lib/net40/FSharpx.Collections.dll"
#r "../../packages/FSharp.Control.AsyncSeq/lib/net45/FSharp.Control.AsyncSeq.dll"
#r "../../packages/FSharpx.Async/lib/net40/FSharpx.Async.dll"
#r "../../packages/FSharpx.Extras/lib/40/FSharpx.Extras.dll"
#load "../../paket-files/Gab-km/msgpack-fsharp/MsgPack/MsgPack.fs"
#load "../../paket-files/Gab-km/msgpack-fsharp/MsgPack/Packer.fs"
#load "../../paket-files/Gab-km/msgpack-fsharp/MsgPack/Unpacker.fs"
#load "Utils.fs"
#load "Types.fs"
#load "Message.fs"
#load "Transport.fs"
#load "EventsDissemination.fs"
#load "Membership.fs"
#load "FailureDetection.fs"
#load "Swim.fs"

open System
open SwimProtocol

let node1 = Swim.init { Swim.defaultConfig with Port = 1337 } []
let node2 = Swim.init { Swim.defaultConfig with Port = 1338 } [ ("mdg-ubuntu", 1337) ]

module MsgPack = 
    [<AutoOpen>]
    module DataType =
        [<Literal>] 
        let Nil = 0xc0uy
        [<Literal>] 
        let False = 0xc2uy
        [<Literal>] 
        let True = 0xc3uy
        [<Literal>] 
        let Binary8 = 0xc4uy
        [<Literal>] 
        let Binary16 = 0xc5uy
        [<Literal>] 
        let Binary32 = 0xc6uy
        [<Literal>]
        let Extension8 = 0xc7uy
        [<Literal>]
        let Extension16 = 0xc8uy
        [<Literal>]
        let Extension32 = 0xc9uy
        [<Literal>]
        let Float32 = 0xcauy
        [<Literal>]
        let Float64 = 0xcbuy
        [<Literal>]
        let UInt8 = 0xccuy
        [<Literal>]
        let UInt16 = 0xcduy
        [<Literal>]
        let UInt32 = 0xceuy
        [<Literal>]
        let UInt64 = 0xcfuy
        [<Literal>]
        let Int8 = 0xd0uy
        [<Literal>]
        let Int16 = 0xd1uy
        [<Literal>]
        let Int32 = 0xd2uy
        [<Literal>]
        let Int64 = 0xd3uy
        [<Literal>]
        let FixExtension1 = 0xd4uy
        [<Literal>]
        let FixExtension2 = 0xd5uy
        [<Literal>]
        let FixExtension4 = 0xd6uy
        [<Literal>]
        let FixExtension8 = 0xd7uy
        [<Literal>]
        let FixExtension16 = 0xd8uy
        [<Literal>]
        let String8 = 0xd9uy
        [<Literal>]
        let String16 = 0xdauy
        [<Literal>]
        let String32 = 0xdbuy
        [<Literal>]
        let Array16 = 0xdcuy
        [<Literal>]
        let Array32 = 0xdduy
        [<Literal>]
        let Map16 = 0xdeuy
        [<Literal>]
        let Map32 = 0xdfuy

    type Value =
        | Nil
        | True | False
        | Int8 of sbyte | Int16 of int16 | Int32 of int | Int64 of int64
        | UInt8 of byte | UInt16 of uint16 | UInt32 of uint32 | UInt64 of uint64

    //let (|Int8|Int16|Int32|Int64|_|) bytes =
    //    None
    //
    //let (|UInt8|UInt16|UInt32|UInt64|_|) bytes =
    //    None

    module private Packer =

        let encodeUInt bytes = 
            let rec pack bytes' = 
                function
                | 2 when Array.get bytes' 1 = 0uy -> Array.take 1 bytes'
                | x when Array.get bytes' (x - 1) = 0uy && Array.get bytes' (x - 2) = 0uy -> pack bytes' (x - 2)
                | x when Array.get bytes' (x - 1) = 255uy && Array.get bytes' (x - 2) = 255uy -> pack bytes' (x - 2)
                | x -> Array.take x bytes'

            let res = pack bytes (Array.length bytes)
            match Array.length res with
            | 1 -> [| yield DataType.UInt8; yield! res |]
            | 2 -> [| yield DataType.UInt16; yield! res |]
            | 4 -> [| yield DataType.UInt32; yield! res |]
            | 8 -> [| yield DataType.UInt64; yield! res |]
            | _ -> failwith "Cannot happen. failing encode uint"
    
        let encodeInt bytes = 
            let rec pack bytes' = 
                function 
                | 2 when Array.get bytes' 0 < 128uy && Array.get bytes' 1 = 0uy -> Array.take 1 bytes'
                | 2 when Array.get bytes' 0 > 127uy && Array.get bytes' 1 = 255uy -> Array.take 1 bytes'
                | x when Array.get bytes' (x - 1) = 0uy && Array.get bytes' (x - 2) = 0uy -> pack bytes' (x - 2)
                | x when Array.get bytes' (x - 1) = 255uy && Array.get bytes' (x - 2) = 255uy -> pack bytes' (x - 2)
                | x -> Array.take x bytes'

            let res = pack bytes (Array.length bytes)
            match Array.length res with
            | 1 -> [| yield DataType.Int8; yield! res |]
            | 2 -> [| yield DataType.Int16; yield! res |]
            | 4 -> [| yield DataType.Int32; yield! res |]
            | 8 -> [| yield DataType.Int64; yield! res |]
            | _ -> failwith "Cannot happen. failing encode int"

        let encode = function
        | Nil -> [|DataType.Nil|]
        | True -> [|DataType.True|]
        | False -> [|DataType.False|]
        | UInt8 b -> encodeUInt [|b|]
        | UInt16 i -> i |> BitConverter.GetBytes |> encodeUInt
        | UInt32 i -> i |> BitConverter.GetBytes |> encodeUInt
        | UInt64 i -> i |> BitConverter.GetBytes |> encodeUInt
        | Int8 b -> encodeInt [|b|]
        | Int16 i -> i |> BitConverter.GetBytes |> encodeInt
        | Int32 i -> i |> BitConverter.GetBytes |> encodeInt
        | Int64 i -> i |> BitConverter.GetBytes |> encodeInt
        | _ -> failwith "Not yet implemented"
  
    module private Unpacker =

        let decode bytes =
            let rec decode' bytes' = seq {
                match Array.head bytes' with
                | DataType.Nil ->
                    yield Value.Nil
                    yield! Array.tail bytes' |> decode'
                | DataType.True ->
                    yield Value.True
                    yield! Array.tail bytes' |> decode'
                | DataType.False ->
                    yield Value.False
                    yield! Array.tail bytes' |> decode'

                | _ -> failwith "Unknow encoding"

            }

            decode' bytes
    
    let pack (l : #seq<Value>) = l |> Seq.collect Packer.encode

    let unpack = Array.map Unpacker.decode

    let values = [ Value.True, Value.Nil, Value.UInt32 254u ]
    pack values