 #r "../../packages/FSharpx.Collections/lib/net40/FSharpx.Collections.dll"
 #r "../../packages/FSharp.Control.AsyncSeq/lib/net45/FSharp.Control.AsyncSeq.dll"
 #r "../../packages/FSharpx.Async/lib/net40/FSharpx.Async.dll"
 #r "../../packages/FSharpx.Extras/lib/40/FSharpx.Extras.dll"
 #load "Utils.fs"
 #load "Types.fs"
 #load "Message.fs"
 #load "Transport.fs"
 #load "EventsDissemination.fs"
 #load "Membership.fs"
 #load "FailureDetection.fs"
 #load "Swim.fs"


open SwimProtocol

let node1 = Swim.init { Swim.defaultConfig with Port = 1337 } []
let node2 = Swim.init { Swim.defaultConfig with Port = 1338 } [ ("mdg-ubuntu", 1337) ]
