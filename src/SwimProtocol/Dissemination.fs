module SwimProtocol.Dissemination

type EventDisseminator =
    private { Agent : MailboxProcessor<obj> }

    with
        member x.Push msg = printfn "[Event push] %A" msg

        member x.Pick numMembers = [||]

        member x.Listen() = ()

let create() = { Agent = new MailboxProcessor<obj>(fun box -> async { () }) }
