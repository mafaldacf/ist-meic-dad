using Boney;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Server = Grpc.Core.Server;
using Timer = System.Timers.Timer;

class ServerMain{

    public static void usage(){
        Console.WriteLine("Invalid arguments passed");
        Console.WriteLine("Usage: dotnet run <configuration_file> <server_number>");
        Console.WriteLine("Configuration file -> the file that has the system's configuration");
        Console.WriteLine("Server number -> Number of the server (to distinguish while reading the configuration file");

    }

    public static void Main(string[] argv){

        if (argv.Length != 2){
            usage();
            return;
        }

        string[] lines;
        string address = "";
        int id = -1,slot_duration =-1,n_slots = -1,k=0;
        string start_time = "";
        List<Tuple<string, int>> servers = new List<Tuple<string, int>>();
        List<List<Tuple<string,string>>> slotInfo= new List<List<Tuple<string,string>>>();

        try{
            lines = System.IO.File.ReadAllLines(argv[0]);
            int target = Int32.Parse(argv[1]);
            for (int i = 0; i < lines.Length; i++){
                string[] split = lines[i].Split(' ');

                if (split.Length >= 3 && split[2] == "boney"){
                    if (i == target-1){
                        address = split[3];
                        id = Int32.Parse(split[1]);
                    }
                    else{
                        servers.Add(new Tuple<string, int>(split[3], Int32.Parse(split[1])));
                    }
                    
                    
                }
                else{

                    switch (split[0]){
                        case "S":
                            n_slots = Int32.Parse(split[1]);
                            break;
                        case "D":
                            slot_duration = Int32.Parse(split[1]);
                            break;
                        case "T":
                            start_time = split[1];
                            break;
                        case "F":
                            slotInfo.Add(new List<Tuple<string,string>>());
                            string[] new_split = lines[i].Split('(');
                            
                            for (int j = 0; j < servers.Count+1; j++){
                                string aux = new_split[j + 1].Replace(" ","");

                                string[] vec = aux.Substring(0, aux.Length - 1).Split(',');


                                if (vec[1] != "N" && vec[1] != "F") throw new FormatException();

                                if (vec[2] != "NS" && vec[2] != "S")throw new FormatException();
                                    slotInfo[k].Add(new Tuple<string, string>(vec[1],vec[2]));
                            }
                            k++;
                            break;
                        case "P":
                            break;
                        default:
                            Console.WriteLine("Command Unknown {0}",split[0]);
                            return;
                    }
                }

            }

           
        }
        catch (System.IO.FileNotFoundException e){
            Console.WriteLine("Invalid configuration file passed");
            Console.WriteLine(e);
            return;
        }
        catch (System.IO.IOException e){
            Console.WriteLine("Invalid configuration file passed");
            Console.WriteLine(e);
            return;

        }

        catch (OverflowException e){
            Console.WriteLine("Invalid number passed");
            Console.WriteLine(e);
            return;
        }
        catch (FormatException e){
            Console.WriteLine("Invalid number passed or invalid slot status");
            Console.WriteLine(e);
            return;
        }

        if (id == -1 || address == "" || n_slots == -1 || slot_duration == -1
            || start_time == "" || k != n_slots){
            Console.WriteLine("There was a problem with the configuration file or the " +
                              "arguments passed.\nID is {0} and " +
                              "and address is {1}\nNote that server's number start with 1",id,address);
            return;
        }


        
        Grpc.Core.Server server;
        Boney.Server boneyServer = new Boney.Server(servers, id, slot_duration, slotInfo);
        PaxosServiceImpl impl = new PaxosServiceImpl(boneyServer);
        try{
            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            string[] split = address.Split(':');
            
            ServerPort serverPort = new ServerPort(split[1].Substring(2), Int32.Parse(split[2]),
                ServerCredentials.Insecure);
            

            server = new Server{
                Services = { PaxosService.BindService(impl).Intercept(new PaxosServerInterceptor()),BoneyService.BindService( 
                    new BoneyServiceImpl(boneyServer)).Intercept(new BoneyServerInterceptor())},
                Ports = { serverPort }
            };

            
        }
        catch (Exception e){
            Console.WriteLine(e);
            return;
        }
        
        DateTime dateTime = DateTime.ParseExact(start_time, "HH:mm:ss",
            System.Globalization.CultureInfo.InvariantCulture);
        
        Boney.Server s = impl._Server;
        var span = dateTime - DateTime.Now;
        var timer = new Timer {Interval = span.TotalMilliseconds, AutoReset = false};
        timer.Elapsed += (sender, e) => {
            server.Start();
            s.startTimer();
            Console.WriteLine("Started server for address {0} with id {1}",address,id);
        };
        timer.Start();
        
        Console.WriteLine("Press any key to stop the server...");
        Console.ReadKey();

        server.ShutdownAsync().Wait();

    }
    
}

