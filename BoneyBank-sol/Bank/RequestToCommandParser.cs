using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bank {
    public static class RequestToCommandParser {

        public static Command parseDepositRequest(DepositRequest request) {
            return new Command(new Tuple<long, long>(request.Credentials.Id, request.Credentials.SequenceNumber),
                               Command.RequestType.Deposit, 
                               request.Value);
        }

        public static Command parseReadBalanceRequest(ReadBalanceRequest request) {
            return new Command(new Tuple<long, long>(request.Credentials.Id, request.Credentials.SequenceNumber),
                               Command.RequestType.ReadBalance,
                               -1);
        }

        public static Command parseWithdrawalRequest(WithdrawalRequest request) {
            return new Command(new Tuple<long, long>(request.Credentials.Id, request.Credentials.SequenceNumber),
                               Command.RequestType.Withdrawal,
                               request.Value);
        }

        /*
         * Cleanup procedure
         */

        public static Command parseCollectedRequest(CollectedRequest request) {
            return new Command(new Tuple<long, long>(request.Id[0], request.Id[1]));
        }
    }
}
