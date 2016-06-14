module MsgPack 

open System

module private DataType =
    let FixMap = [| 0x80uy..0x8fuy |]
    let FixArray = [| 0x90uy..0x9fuy |]
    let FixString = [| 0xa0uy..0xbfuy |]
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
    | Bool of bool
    | Int8 of sbyte | Int16 of int16 | Int32 of int | Int64 of int64
    | UInt8 of byte | UInt16 of uint16 | UInt32 of uint32 | UInt64 of uint64
    | Float32 of float32 | Float64 of float
    | String of string
    | Binary of byte[]
    | Array of Value[]
    | Map of Map<Value, Value>
    | Extension of int * byte[]

[<AutoOpen>]
module Values =
    let (|UInt8|_|) = function UInt8 b -> Some b | _ -> None
    let (|UInt16|_|) = function 
        | UInt8 b -> uint16 b |> Some
        | UInt16 i -> Some i
        | _ -> None
    let (|UInt32|_|) = function 
        | UInt8 b -> uint32 b |> Some
        | UInt16 i -> uint32 i |> Some
        | UInt32 i -> Some i
        | _ -> None
    let (|UInt64|_|) = function 
        | UInt8 b -> uint64 b |> Some
        | UInt16 i -> uint64 i |> Some
        | UInt32 i -> uint64 i |> Some
        | UInt64 i -> Some i
        | _ -> None
       
    let (|Int8|_|) = function Int8 b -> Some b | _ -> None
    let (|Int16|_|) = function 
        | Int8 b -> int16 b |> Some
        | Int16 i -> Some i
        | _ -> None
    let (|Int32|_|) = function 
        | Int8 b -> int32 b |> Some
        | Int16 i -> int32 i |> Some
        | Int32 i -> Some i
        | _ -> None
    let (|Int64|_|) = function 
        | Int8 b -> int64 b |> Some
        | Int16 i -> int64 i |> Some
        | Int32 i -> int64 i |> Some
        | Int64 i -> Some i
        | _ -> None

    let (|Float32|_|) = function Float32 f -> Some f | _ -> None
    let (|Float64|_|) = function 
        | Float32 f -> float f |> Some
        | Float64 f -> Some f
        | _ -> None

module Packer =
    let private packUInt bytes = 
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

    let private packInt bytes = 
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

    let rec pack = function
        | Nil -> [| DataType.Nil |]
        | Bool b when b -> [| DataType.True |]
        | Bool _ -> [| DataType.False |]
        | UInt8 b -> packUInt [| b |]
        | UInt16 i -> i |> BitConverter.GetBytes |> packUInt
        | UInt32 i -> i |> BitConverter.GetBytes |> packUInt
        | UInt64 i -> i |> BitConverter.GetBytes |> packUInt
        | Int8 b -> packInt [| byte b |]
        | Int16 i -> i |> BitConverter.GetBytes |> packInt
        | Int32 i -> i |> BitConverter.GetBytes |> packInt
        | Int64 i -> i |> BitConverter.GetBytes |> packInt
        | Float32 f -> [| yield DataType.Float32; yield! BitConverter.GetBytes f |]
        | Float64 f -> [| yield DataType.Float64; yield! BitConverter.GetBytes f |]
        | String s -> [|  |]
        | Array a when Array.length a < 16 ->
            [| yield DataType.FixArray.[Array.length a]
               yield! Array.collect pack a |] 
        
    let packArray = Value.Array >> pack
        
module Unpacker =
    let private unpackUInt8 bytes =
        let value = Array.head bytes
        UInt8 value, Array.tail bytes
        
    let private unpackUInt16 bytes =
        let values = Array.take 2 bytes
        BitConverter.ToUInt16(values, 0) |> UInt16, 
        bytes |> Array.skip 2
        
    let private unpackUInt32 bytes =
        let values = Array.take 4 bytes
        BitConverter.ToUInt32(values, 0) |> UInt32, 
        bytes |> Array.skip 4

    let unpack bytes =
        let rec decode' bytes' = [|
            if Array.length bytes' > 0 then
                match Array.head bytes' with
                | DataType.Nil ->
                    yield Value.Nil
                    yield! Array.tail bytes' |> decode'
                | DataType.True ->
                    yield Value.Bool true
                    yield! Array.tail bytes' |> decode'
                | DataType.False ->
                    yield Value.Bool false
                    yield! Array.tail bytes' |> decode'
                | DataType.UInt8 ->
                    let value, tail = bytes' |> Array.tail |> unpackUInt8
                    yield value
                    yield! tail |> decode'
                | DataType.UInt16 ->
                    let value, tail = bytes' |> Array.tail |> unpackUInt16
                    yield value
                    yield! tail |> decode'
                | DataType.UInt32 ->
                    let value, tail = bytes' |> Array.tail |> unpackUInt32
                    yield value
                    yield! tail |> decode'
                | DataType.Float32 ->
                    let tail = bytes' |> Array.tail
                    yield BitConverter.ToSingle(tail, 0) |> Float32 
                    yield! Array.skip 4 tail |> decode'
                | DataType.Float64 ->
                    let tail = bytes' |> Array.tail
                    yield BitConverter.ToDouble(tail, 0) |> Float64 
                    yield! Array.skip 8 tail |> decode'
//                | DataType.String8
//                | DataType.String16
//                | DataType.String32
//                | DataType.Array16
//                | DataType.Array32
                | b when Array.exists ((=) b) DataType.FixArray ->
                    let cnt = int b
                    yield bytes' |> Array.tail |> Array.take cnt |> decode' |> Array
                    yield! Array.skip (cnt + 1) bytes' |> decode'
                | _ -> failwith "Unknow encoding"
        |]

        decode' bytes
