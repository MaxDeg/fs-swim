namespace SwimProtocol

module EventsDissemination = 
    open System
    
    type T = 
        private
        | T of MailboxProcessor<obj>
    
    let create() = new MailboxProcessor<obj>(fun box -> async { () }) |> T

    let push (T agent) msg = 
        printfn "[Event push] %A" msg
        ()

    // Add a new event to the list
    let pick (T agent) numMembers = [||]
    
    // Pick up the most recent event to disseminate
    let listen (T agent) = 
        { new IObservable<MembershipEvent> with
              member __.Subscribe _ = 
                  { new IDisposable with
                        member __.Dispose() = () } }
