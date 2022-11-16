using System.Globalization;
using Timer = System.Timers.Timer;

namespace Clients {

    public class ClientMain {

        // Usage example: dotnet run configuration_sample.txt 7 < ../../../Scripts/bank_client_script_sample.txt
        public static void usage() { //
            Console.WriteLine("Usage: dotnet run <configuration_file> <client_number>");
            Console.WriteLine("Configuration file -> the file that has the system's configuration");
            Console.WriteLine("Client number -> Number of the client (to identify requests with client credentials)");

        }

        public static void setTimer(Client client, string startTime) {
            DateTime dateTime = DateTime.ParseExact(startTime, "HH:mm:ss", CultureInfo.InvariantCulture);
            var span = dateTime - DateTime.Now;

            if (span.TotalMilliseconds > 0) {
                var timer = new Timer {
                    Interval = span.TotalMilliseconds,
                    AutoReset = false
                };
                timer.Elapsed += (sender, e) => {
                    Console.WriteLine("Servers are now available!");
                    client.Start();
                };

                timer.Start();
            } else {
                client.Start();
            }
        }

        public static void LoadConfig(Client client, string config) {
            int count = 0;
            if (File.Exists(config)) {
                string[] lines = File.ReadAllLines(config);
                foreach (string line in lines) {
                    if (!string.IsNullOrEmpty(line)) {
                        string[] items = line.Split(' ');
                        if (items.Length == 4 && items[0].Equals("P") && items[2].Equals("bank")) {
                            count++;
                            client.AddServer(int.Parse(items[1]), items[3]);
                        }

                        else if (items.Length == 4 && items[0].Equals("P") 
                            && items[2].Equals("client") && int.Parse(items[1]).Equals(client.id)) {
                            client.setScript(items[3]);
                        }

                        else if (items.Length == 2 && items[0].Equals("T")) {
                            setTimer(client, items[1]);
                        } 
                    }
                }
            }
        }

        public static void Main(string[] args) {
            if (args.Length != 2) {
                usage();
                return;
            }

            try {
                string config = args[0];
                int id = int.Parse(args[1]);
                Client client = new Client(id);
                LoadConfig(client, config);
                Console.WriteLine("Press any key to stop the client...");
                Console.ReadKey();

            } catch (Exception e) when (e is ArgumentNullException || e is FormatException || e is OverflowException) {
                Console.WriteLine("Invalid client number.");
                Console.WriteLine(e);
                return;
            }
        }
    }
}
