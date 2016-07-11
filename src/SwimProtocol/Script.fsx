open System
open System.Diagnostics

let localName = System.Net.Dns.GetHostName()

let exec args =
    let psi = new ProcessStartInfo(__SOURCE_DIRECTORY__ + @"\bin\Release\SwimProtocol.exe")
    psi.Arguments <- args
    psi.UseShellExecute <- true
    Process.Start(psi) |> ignore

let remoteNode = sprintf "%s:%i"

let execOtherNode port =
    remoteNode "127.0.0.1" 1337
    |> sprintf "%s %s" port
    |> exec

exec "1337"
execOtherNode "1338"
execOtherNode "1339"
execOtherNode "1340"
execOtherNode "1341"
