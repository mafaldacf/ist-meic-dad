using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Timer = System.Timers.Timer;

namespace Bank {

    public class BankMain {

        public static void usage() {
            Console.WriteLine("Invalid arguments passed");
            Console.WriteLine("Usage: dotnet run <configuration_file> <server_number>");
            Console.WriteLine("Configuration file -> the file that has the system's configuration");
            Console.WriteLine("Server number -> Number of the server (to distinguish while reading the configuration file");

        }


        public static void Main(string[] args) {

            if (args.Length != 2) {
                usage();
                return;
            }

            string[] lines;
            string address = "";
            int id = -1, k = 0;
            int slotDuration = -1, nrOfSlots = -1;
            string startTime = "";
            Dictionary<int, PrimaryBackupClient> bankServers = new Dictionary<int, PrimaryBackupClient>();
            Dictionary<int, BoneyService.BoneyServiceClient> boneyServers = new Dictionary<int, BoneyService.BoneyServiceClient>();
            List<SortedDictionary<int, List<string>>> slotsInfo = new List<SortedDictionary<int, List<string>>>();

            try {
                lines = System.IO.File.ReadAllLines(args[0]);
                int serverNr = Int32.Parse(args[1]);

                for (int i = 0; i < lines.Length; i++) {
                    string[] split = lines[i].Split(' ');

                    if (split.Length == 4 && split[2] == "bank") {
                        if (Int32.Parse(split[1]) == serverNr) {
                            address = split[3];
                            id = Int32.Parse(split[1]);
                        }
                        else {
                            GrpcChannel channel = GrpcChannel.ForAddress(split[3]);
                            PrimaryBackupClient bank = new PrimaryBackupClient(split[3], Int32.Parse(split[1]));
                            bankServers.Add(Int32.Parse(split[1]), bank);
                        }
                    }

                    else if (split.Length == 4 && split[2] == "boney") {
                        var interceptor = new ClientInterceptor();
                        GrpcChannel channel = GrpcChannel.ForAddress(split[3]);
                        CallInvoker interceptingInvoker = channel.Intercept(interceptor);
                        BoneyService.BoneyServiceClient boneyServer = new BoneyService.BoneyServiceClient(interceptingInvoker);
                        boneyServers.Add(Int32.Parse(split[1]), boneyServer);
                    }

                    else {

                        switch (split[0]) {
                            case "S":
                                nrOfSlots = Int32.Parse(split[1]);
                                break;
                            case "T":
                                startTime = split[1];
                                break;
                            case "D":
                                slotDuration = Int32.Parse(split[1]);
                                break;
                            case "F":
                                slotsInfo.Add(new SortedDictionary<int, List<string>>());
                                string[] newSplit = lines[i].Split('(');

                                // bank servers info is after boney servers info; counts backwards
                                for (int j = newSplit.Length - 1; j > boneyServers.Count; j--) {

                                    string aux = newSplit[j].Replace(" ", "");
                                    string[] vec = aux.Substring(0, aux.Length-1).Split(',');

                                    if (vec[1] != "N" && vec[1] != "F")
                                        throw new FormatException();
                                    if (vec[2] != "S" && vec[2] != "NS")
                                        throw new FormatException();

                                    slotsInfo[k].Add(Int32.Parse(vec[0]), new List<string>() { vec[1], vec[2] });
                                }
                                k++;
                                break;
                        }
                    }

                }

            } catch (System.IO.FileNotFoundException e) {
                Console.WriteLine("Invalid configuration file passed");
                Console.WriteLine(e);
                return;

            } catch (System.IO.IOException e) {
                Console.WriteLine("Invalid configuration file passed");
                Console.WriteLine(e);
                return;

            } catch (OverflowException e) {
                Console.WriteLine("Invalid number passed");
                Console.WriteLine(e);
                return;

            } catch (FormatException e) {
                Console.WriteLine("Invalid number passed or invalid slot status");
                Console.WriteLine(e);
                return;
            }


            if (id == -1 || address == "") {
                Console.WriteLine("There was a problem with the configuration file or the " +
                              "arguments passed.\nID is {0} " +
                              "and address is {1}\nNote that server's number start with 1", id, address);
                return;
            }



            Server server;
            BankServer bankServer = new BankServer(bankServers, boneyServers, slotsInfo, slotDuration, id);
            BankServerImpl bankServerImpl = new BankServerImpl(bankServer);
            PrimaryBackupServiceImpl primaryBackupServiceImpl = new PrimaryBackupServiceImpl(bankServer);

            try {

                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

                string[] split = address.Split(':');
                ServerPort serverPort = new ServerPort(split[1].Substring(2), Int32.Parse(split[2]), ServerCredentials.Insecure);

                server = new Server {
                    Services = { BankServerService.BindService(bankServerImpl).Intercept(new ServerInterceptor()), 
                        PrimaryBackupService.BindService(primaryBackupServiceImpl).Intercept(new ServerInterceptor()) },
                    Ports = { serverPort }
                };


            }  catch (Exception e) {
                Console.WriteLine(e);
                return;
            }

            DateTime dateTime = DateTime.ParseExact(startTime, "HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            var span = dateTime - DateTime.Now;

            var timer = new Timer {
                Interval = span.TotalMilliseconds,
                AutoReset = false
            };
            // To start the server now -> change the start time in config file
            timer.Elapsed += (sender, e) => {
                Console.WriteLine("Started server for address {0} with id {1}", address, id);
                server.Start();
                bankServer.startTimer();
            };

            timer.Start();

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}
