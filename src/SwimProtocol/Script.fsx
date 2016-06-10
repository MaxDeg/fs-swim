#r @"..\..\packages\Rx-Core\lib\net45\System.Reactive.Core.dll"
#r @"..\..\packages\Rx-Linq\lib\net45\System.Reactive.Linq.dll"
#r @"..\..\packages\Rx-Interfaces\lib\net45\System.Reactive.Interfaces.dll"
#r @"..\..\packages\FSharp.Control.Reactive\lib\net45\FSharp.Control.Reactive.dll"
#r @"..\..\packages\FSharpx.Extras\lib\40\FSharpx.Extras.dll"
#r @"..\..\packages\FSharpx.Collections\lib\net40\FSharpx.Collections.dll"
#r @"..\..\packages\FSharpx.Async\lib\net40\FSharpx.Async.dll"
#r @"..\..\packages\FSharp.Control.AsyncSeq\lib\net45\FSharp.Control.AsyncSeq.dll"

#load @"..\..\paket-files\Gab-km\msgpack-fsharp\MsgPack\MsgPack.fs"
#load @"..\..\paket-files\Gab-km\msgpack-fsharp\MsgPack\Packer.fs"
#load @"..\..\paket-files\Gab-km\msgpack-fsharp\MsgPack\Unpacker.fs"
#load "Utils.fs"
#load "Types.fs"
#load "Transport.fs"
#load "EventsDissemination.fs"
#load "Membership.fs"
#load "FailureDetection.fs"
#load "Swim.fs"

open SwimProtocol

let node1 = Swim.init { Swim.defaultConfig with Port = 1337 } []
let node2 = Swim.init { Swim.defaultConfig with Port = 1338 } [ ("mdg-7", 1337) ]
