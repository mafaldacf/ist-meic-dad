using Grpc.Core;

namespace Bank {
    public class BankServerImpl : BankServerService.BankServerServiceBase {

        private BankServer bankServer;
        private float balance = 0;
        private Object balance_lock = new Object();


        public BankServerImpl(
           BankServer bankServer) {

            this.bankServer = bankServer;
        }

        public ServerInfo getServerInfo() {
            return bankServer.isLeader() == true ? ServerInfo.Primary : ServerInfo.Backup;
        }


        public override Task<ReadBalanceReply> ReadBalance(ReadBalanceRequest request, ServerCallContext context) {
            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }
            Console.WriteLine($"\nReceived a read balance request {request.Credentials}");
            return Task.FromResult(ReadBalance(request));
        }

        public ReadBalanceReply ReadBalance(ReadBalanceRequest request) {
            float balance;
            ClientCredentials credentials = request.Credentials;

            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }

            lock (bankServer) {
                bankServer.addWaitingCommand(RequestToCommandParser.parseReadBalanceRequest(request));
                while (!bankServer.inCommited(credentials) && !bankServer.isLastCommited(credentials)) {
                    Monitor.Wait(bankServer);
                }
                lock (balance_lock) {
                    balance = this.balance;
                }
            }

            return new ReadBalanceReply {
                Balance = balance,
                ServerInfo = getServerInfo()
            };
        }


        public override Task<DepositReply> Deposit(DepositRequest request, ServerCallContext context) {
            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }
            Console.WriteLine($"\nReceived a deposit request {request.Credentials}");
            return Task.FromResult(Deposit(request));
        }

        public DepositReply Deposit(DepositRequest request) {
            float balance;
            ClientCredentials credentials = request.Credentials;

            lock (bankServer) {
                bankServer.addWaitingCommand(RequestToCommandParser.parseDepositRequest(request));
                while (!bankServer.inCommited(credentials) && !bankServer.isLastCommited(credentials)) {
                    Monitor.Wait(bankServer);
                }
                lock (balance_lock) {
                    this.balance += request.Value;
                    balance = this.balance;
                }
            }

            return new DepositReply {
                Balance = balance,
                ServerInfo = getServerInfo()
            };
        }


        public override Task<WithdrawalReply> Withdrawal(WithdrawalRequest request, ServerCallContext context) {
            if (bankServer.isFrozen()) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Frozen"));
            }
            Console.WriteLine($"\nReceived a withdrawal request {request.Credentials}");
            return Task.FromResult(Withdrawal(request));
        }

        public WithdrawalReply Withdrawal(WithdrawalRequest request) {
            float balance, value = request.Value;
            ClientCredentials credentials = request.Credentials;

            lock (bankServer) {
                bankServer.addWaitingCommand(RequestToCommandParser.parseWithdrawalRequest(request));
                while (!bankServer.inCommited(credentials) && !bankServer.isLastCommited(credentials)) {
                    Monitor.Wait(bankServer);
                }
                lock (balance_lock) {
                    if (this.balance - value >= 0) {
                        this.balance -= value;
                    } else {
                        value = 0;
                    }
                    balance = this.balance;
                }
            }

            return new WithdrawalReply {
                Value = value,
                Balance = balance,
                ServerInfo = getServerInfo()
            };
        }

    }
}
