using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace Bank {
    public class PrimaryBackupClient {

        private PrimaryBackupService.PrimaryBackupServiceClient target;

        public delegate void ReplyHandler(bool ack);
        public delegate void CleanupHandler(ListPendingRequestsReply reply);

        public PrimaryBackupClient(string address, int id) {

            AppContext.SetSwitch(
            "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            var interceptor = new ClientInterceptor();
            var channel = GrpcChannel.ForAddress(address);
            CallInvoker interceptingInvoker = channel.Intercept(interceptor);
            target = new PrimaryBackupService.PrimaryBackupServiceClient(interceptingInvoker);
        }

        public void tentative(TentativeRequest request, ReplyHandler replyHandler) {
            AcknowledgeReply reply = this.target.Tentative(request);
            replyHandler(reply.Acknowledge);
        }

        public void commit(CommitRequest request) {
            CommitReply reply = this.target.Commit(request);
        }

        public RecoverStateReply recoverState(RecoverStateRequest request) {
            return this.target.RecoverState(request);
        }

        public void listPendingRequests(ListPendingRequestsRequest request, CleanupHandler cleanupHandler) {
            ListPendingRequestsReply reply = this.target.ListPendingRequests(request);
            cleanupHandler(reply);
        }
    }
}
