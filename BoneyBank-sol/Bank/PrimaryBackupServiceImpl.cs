using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;

namespace Bank {
    public class PrimaryBackupServiceImpl : PrimaryBackupService.PrimaryBackupServiceBase {

        private BankServer bankServer;

        public PrimaryBackupServiceImpl(BankServer bankServer) {
            this.bankServer = bankServer;
        }

        public override Task<AcknowledgeReply> Tentative(TentativeRequest request, ServerCallContext context) {
            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }
            return Task.FromResult(Tentative(request));
        }

        public AcknowledgeReply Tentative(TentativeRequest request) {
            return new AcknowledgeReply {
                Acknowledge = this.bankServer.doTentative(request),
            };
        }

        public override Task<CommitReply> Commit(CommitRequest request, ServerCallContext context) {
            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }
            return Task.FromResult(Commit(request));
        }

        public CommitReply Commit(CommitRequest request) {
            return new CommitReply() {
                Acknowledge = this.bankServer.doCommit(request),
            };
        }

        public override Task<RecoverStateReply> RecoverState(RecoverStateRequest request, ServerCallContext context) {
            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }
            return Task.FromResult(RecoverState(request));
        }

        public RecoverStateReply RecoverState(RecoverStateRequest request) {
            return new RecoverStateReply() {
                RecoveredCommit = { this.bankServer.doRecoverState(request) }
            };
        }

        /*
         * Cleanup procedure
         */

        public override Task<ListPendingRequestsReply> ListPendingRequests(ListPendingRequestsRequest request, ServerCallContext context) {
            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }
            return Task.FromResult(ListPendingRequests(request));
        }

        public ListPendingRequestsReply ListPendingRequests(ListPendingRequestsRequest request) {
            Tuple<List<List<CollectedRequest>>, long> pendingCommands = this.bankServer.listPendingRequests(
                request.LastKnownSequenceNumber, request.Slot, request.Leader);

            return new ListPendingRequestsReply() {
                CommitedCommands = { pendingCommands.Item1[0] },
                TentativeCommands = { pendingCommands.Item1[1] },
                LastSequenceNumber = pendingCommands.Item2
            };
        }


    }
}
