#r "../../packages/FSharpx.Collections/lib/net40/FSharpx.Collections.dll"
#r "../../packages/FSharp.Control.AsyncSeq/lib/net45/FSharp.Control.AsyncSeq.dll"
#r "../../packages/FSharpx.Async/lib/net40/FSharpx.Async.dll"
#r "../../packages/FSharpx.Extras/lib/40/FSharpx.Extras.dll"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/DataType.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Value.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Packer.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Unpacker.fs"
#load "Utils.fs"
#load "Types.fs"
#load "Message.fs"
#load "Udp.fs"
#load "Dissemination.fs"
#load "MemberList.fs"
#load "FailureDetection.fs"
#load "Swim.fs"

open SwimProtocol
open System

let localName = System.Net.Dns.GetHostName()

let node1 =
    Swim.start { Swim.defaultConfig with Port = 1337us
                                         PeriodTimeout = TimeSpan.FromSeconds(1.) } []

let node2 =
    Swim.start { Swim.defaultConfig with Port = 1338us
                                         PeriodTimeout = TimeSpan.FromSeconds(1.) } [ (localName, 1337us) ]

let node3 =
    Swim.start { Swim.defaultConfig with Port = 1339us
                                         PeriodTimeout = TimeSpan.FromSeconds(1.) } [ (localName, 1337us) ]

// -------------------------------------------------------------



