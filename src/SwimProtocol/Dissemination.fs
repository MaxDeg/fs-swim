module SwimProtocol.Dissemination

type private Request =
| Push of SwimEvent
| Pull of AsyncReplyChannel<SwimEvent[]>

type EventDisseminator =
    private { Agent : MailboxProcessor<Request> }

    with
        member x.Push msg =
            printfn "[Event push] %A" msg
            Push msg |> x.Agent.Post

        member x.Pull numMembers =
            Pull |> x.Agent.PostAndReply

        member x.Listen() = ()

let create() = { Agent = new MailboxProcessor<Request>(fun box -> async { () }) }
