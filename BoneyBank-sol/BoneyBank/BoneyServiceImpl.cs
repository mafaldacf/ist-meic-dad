using Grpc.Core;

namespace Boney;

public class BoneyServiceImpl : BoneyService.BoneyServiceBase{
    
    private Server server;
    
    public BoneyServiceImpl(Server server){
        this.server = server;
    }

    public override Task<CompareAndSwapReply> compareAndSwap(CompareAndSwapRequest request, ServerCallContext context){
        return  Task.FromResult(compareAndSwap(request));
    }

    public CompareAndSwapReply compareAndSwap(CompareAndSwapRequest request){
        return this.server.start_consensus(request.Slot, request.Leader);
    }
}