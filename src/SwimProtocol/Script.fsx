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
