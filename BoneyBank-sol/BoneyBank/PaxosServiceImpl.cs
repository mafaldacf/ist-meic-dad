using Grpc.Core;

namespace Boney;

public class PaxosServiceImpl : PaxosService.PaxosServiceBase{
    private Server server;

    public Server _Server{
        get{ return server; }
    }
    

    public PaxosServiceImpl(Server server) {
        this.server = server;
    }
    public override Task<CommitReply> commit(CommitRequest request, ServerCallContext context){
        return Task.FromResult(commit((request)));
    }

    public override Task<PromiseReply> propose(ProposeRequest request, ServerCallContext context){
        return Task.FromResult(propose(request));
    }


    public override Task<PrepareReply> read(PrepareRequest request, ServerCallContext context){
        return Task.FromResult(read(request));
    }


    private CommitReply commit(CommitRequest request){
        return new CommitReply{
            Status = this.server.commit(request),
            Slot =request.Slot
        };
    }

    private PromiseReply propose(ProposeRequest request){
        return new PromiseReply{
            Status = this.server.propose(request),
            Slot =request.Slot
        };

    }

    private PrepareReply read(PrepareRequest request){
        return this.server.read(request);

    }
}