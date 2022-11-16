using Grpc.Core.Interceptors;
using Grpc.Core;
using Grpc.Net.Client;

namespace Boney;

public class PaxosClient{
    private PaxosService.PaxosServiceClient target;
    private int id;

    public delegate void ReplyHadler(int value,Server.Paxos_status status,int slot);

    private ReplyHadler reply;
    public PaxosClient(string address,int id,ReplyHadler r){
        
        
        AppContext.SetSwitch(
            "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        var channel = GrpcChannel.ForAddress(address);
        var interceptor = new PaxosClientInterceptor();
        CallInvoker interceptingInvoker = channel.Intercept(interceptor);
        target = new PaxosService.PaxosServiceClient(interceptingInvoker);

        this.id = id;


        this.reply = r;
    }

    public void commit(CommitRequest request){
        CommitReply r = this.target.commit(request);

        if (r.Status == Status.Kill){
            this.reply(-1,Server.Paxos_status.Commit,request.Slot);
        }
        else if(r.Status != Status.Frozen){
            this.reply(1,Server.Paxos_status.Commit,request.Slot);
        }
    }

    public void propose(ProposeRequest request){
        PromiseReply r = this.target.propose(request);
        
        if (r.Status == Status.Kill){
            this.reply(-1,Server.Paxos_status.Propose,request.Slot);
        }
        else if(r.Status != Status.Frozen){
            this.reply(1,Server.Paxos_status.Propose,request.Slot);
        }
    }

    public void read(PrepareRequest request,Action<PrepareReply> f){
        PrepareReply r = this.target.read(request);

        f(r);

    }

}