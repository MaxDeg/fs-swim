# fs-swim
## Swim protocol implementation

This repository contains a implementation of Swim protocol (<https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf>) in F#.

This could be used as a library to keep a cluster of mutiple node connected together with an heartbeat system integrated.

A simple program is provided in the solution.

### Usage

SwimProtocol.exe [port] [node addresses] 


- SwimProtocol.exe "1337" -- Will run the first node of a cluster
- SwimProtocol.exe "1338" "XXX.XXX.XXX.XXX:1337" -- Wil run another that will connect to the cluster

### Example

Running a cluster of 5 nodes on the same machine.

```fsharp
open System
open System.Diagnostics

let localName = System.Net.Dns.GetHostName()

let exec args =
    let psi = new ProcessStartInfo(__SOURCE_DIRECTORY__ + @"\bin\Release\SwimProtocol.exe")
    psi.Arguments <- args
    psi.UseShellExecute <- true
    Process.Start(psi) |> ignore

let remoteNode = sprintf "%s:%i"

exec "1337"

for i in 1..7 do
    remoteNode "127.0.0.1" 1337
    |> sprintf "%i %s" (1337 + i)
    |> exec
```
