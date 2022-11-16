using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Clients {
    public class Client {
        private Dictionary<int, BankServerService.BankServerServiceClient> bankServers;
        private long SequenceNumber;
        private string? scriptPath = null;
        public int id { get; set; }

        

        public Client(int id) {
            bankServers = new Dictionary<int, BankServerService.BankServerServiceClient>();
            this.id = id;
            SequenceNumber = 0;
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        public void setScript(string filename) {
            this.scriptPath = "Scripts/" + filename;
        }

        public void AddServer(int id, string url) {
            var interceptor = new BankClientInterceptor();
            GrpcChannel channel = GrpcChannel.ForAddress(url);
            CallInvoker interceptingInvoker = channel.Intercept(interceptor);
            BankServerService.BankServerServiceClient server = new BankServerService.BankServerServiceClient(interceptingInvoker);
            bankServers.Add(id, server);
        }

        public ClientCredentials getClientCredentials(long sequenceNumber) {
            return new ClientCredentials {
                Id = id,
                SequenceNumber = sequenceNumber
            };
        }

        public void Wait(int time) // time in miliseconds
        {
            Console.WriteLine($"Sleeping for {time} ms...");
            Thread.Sleep(time);
        }

        public void Deposit(float amount) {
            long sequenceNumber = this.SequenceNumber++;
            int i = 0;
            WaitHandle[] waitHandles = new WaitHandle[bankServers.Count];

            foreach (var item in bankServers)
            {
                int bankId = item.Key;
                BankServerService.BankServerServiceClient server = item.Value;
                var handle = new EventWaitHandle(false, EventResetMode.ManualReset);

                Thread t = new Thread(() => {
                    DepositReply reply = server.Deposit(new DepositRequest {
                        Value = amount,
                        Credentials = getClientCredentials(sequenceNumber),
                    });
                    Console.WriteLine($"({sequenceNumber}) Deposit | Bank {bankId} ({reply.ServerInfo}): Deposited {amount}. Balance = {reply.Balance}");

                    handle.Set();
                });
                waitHandles[i++] = handle;
                t.Start();
            }
            WaitHandle.WaitAll(waitHandles, 500);
        }

        public void Withdrawal(float amount) {
            long sequenceNumber = this.SequenceNumber++;
            int i = 0;
            WaitHandle[] waitHandles = new WaitHandle[bankServers.Count];

            foreach (var item in bankServers)
            {
                int bankId = item.Key;
                BankServerService.BankServerServiceClient server = item.Value;
                var handle = new EventWaitHandle(false, EventResetMode.ManualReset);

                Thread t = new Thread(() => {
                    WithdrawalReply reply = server.Withdrawal(new WithdrawalRequest {
                        Value = amount,
                        Credentials = getClientCredentials(sequenceNumber),
                    });
                    if (reply.Value == 0) {
                        Console.WriteLine($"({sequenceNumber}) Withdrawal | Bank {bankId} ({reply.ServerInfo}): " +
                            $"Insufficient Funds. Balance = {reply.Balance}");
                    } else {
                        Console.WriteLine($"({sequenceNumber}) Withdrawal | Bank {bankId} ({reply.ServerInfo}): Withdrew {amount}. " +
                            $"Balance = {reply.Balance}");
                    }

                    handle.Set();
                });

                waitHandles[i++] = handle;
                t.Start();
            }
            WaitHandle.WaitAll(waitHandles, 500);
        }

        public void ReadBalance() {
            long sequenceNumber = this.SequenceNumber++;
            int i = 0;
            WaitHandle[] waitHandles = new WaitHandle[bankServers.Count];

            foreach (var item in bankServers) {
                int bankId = item.Key;
                BankServerService.BankServerServiceClient server = item.Value;
                var handle = new EventWaitHandle(false, EventResetMode.ManualReset);

                Thread t = new Thread(() => {
                    ReadBalanceReply reply = server.ReadBalance(new ReadBalanceRequest {
                        Credentials = getClientCredentials(sequenceNumber),
                    });
                    Console.WriteLine($"({sequenceNumber}) ReadBalance | Bank {bankId} ({reply.ServerInfo}): Balance = {reply.Balance}");

                    handle.Set();
                });

                waitHandles[i++] = handle;
                t.Start();
            }
            WaitHandle.WaitAll(waitHandles, 500);
        }

        public void Start() {

            // read commands from script file
            if (this.scriptPath != null && File.Exists(this.scriptPath)) {
                Console.WriteLine($"Reading script file at {this.scriptPath}...");
                string[] lines = File.ReadAllLines(this.scriptPath);
                foreach (string command in lines) {
                    if (!string.IsNullOrEmpty(command)) {
                        Console.WriteLine("> " + command);
                        int status = parseCommand(command);
                        if (status == -1) Environment.Exit(0);
                    }
                }
            }

            Console.WriteLine("Enter a new command: R | D <amount> | W <amount> | S <sleeptime> | exit | help");

            // read commands from standard input
            while (true) {
                Console.Write("> ");
                string? command = Console.ReadLine();
                int status = parseCommand(command);
                if (status == -1) Environment.Exit(0);
            }
        }

        public int parseCommand(string? command) {
            float amount;
            int time;
            string pattern = "([DW] [0-9]*.?[0-9]?[0-9]?)|(S [0-9]*)|R|help|exit";

            try {

                if (command == null || !Regex.IsMatch(command, pattern)) {
                    Console.WriteLine("Invalid commmand. Type 'help' to see further options.");
                    return 0;
                }

                string[] commandItems = command.Split(' ');

                switch (commandItems[0]) {
                    case "D":
                        amount = float.Parse(commandItems[1], CultureInfo.InvariantCulture);
                        Deposit(amount);
                        break;

                    case "W":
                        amount = float.Parse(commandItems[1], CultureInfo.InvariantCulture);
                        Withdrawal(amount);
                        break;

                    case "R":
                        ReadBalance();
                        break;

                    case "S":
                        time = int.Parse(commandItems[1]);
                        Wait(time);
                        break;

                    case "help":
                        Console.WriteLine("Available commands:\n- Deposit: D <amount>\n- Withdrawal: W <amount>\n- Read Balance: R\n- Help: help\n- Exit: exit");
                        break;

                    case "exit":
                        return -1;
                }
            } catch (Exception e) when (
                e is ArgumentNullException ||
                e is ArgumentException ||
                e is FormatException || 
                e is OverflowException) {
                Console.WriteLine("Invalid command parameter.");
                Console.WriteLine(e);
            } catch (RegexMatchTimeoutException e) {
                Console.WriteLine("Command is not available.");
                Console.WriteLine(e);
            }

            return 0;
        }
    }
}
