Instituto Superior Técnico

Master's Degree in Computer Science and Engineering

Design and Implementation of Distributed Applications 2022/2023

Project Grade: 18.50 / 20.00

# Boney Bank, a fault-tolerant bank application

The goal of the Boney Bank project is to develop a three tier system that emulates the behaviour of simple but reliably fault-tolerant bank application. Boney Bank has a first tier composed of clients who deposit and withdraw money from a banking account. The operations are submitted to a set of bank server processes, the second tier, that use primary-backup replication to ensure the durability of the account information. Finally, in the third tier, is the Boney system, a distributed coordination system running on top of Paxos that the bank server processes use to determine which one of the bank servers in the second tier is the primary server.

The project was implemented using C# and .Net Framework.

## Authors

Group 11

92513 Mafalda Ferreira

95585 Guilherme Gonçalves

95637 Marina Gomes

## Requirements

- Microsoft Visual Studio
- .Net Core 3.1+

## Setting up the system in Windows/iOS

1. Set up text files of clients' scripts in `BoneyBank-sol/Clients/Scripts` to execute bank operations: read balance `R`, deposit `D <amount>`, withdrawal `W <amount>` and sleep `S <sleeptime>` to block the application's main thread for a given time. Alternatively, if no script is provided in (3), commands can be issued via standard input of the client program.

2. Set up the global "wall" time of the beggining of the first slot (command T) in `BoneyBank-sol/Configurations/configuration_sample.txt` configuration file.

3. Specify the client script for each client process (command P) in the previous configuration file.

## Running the system

By default, if any configuration is changed the system will run **3 BoneyBank servers**, **5 Bank servers** and **3 Clients**.
All three clients will be using `client_script_4.txt` scripts.

Change the current working directory to the system's solution:

``` zsh
cd BoneyBank-sol
```

Starting from the solution's directory, run 3 Boney Bank Servers with id's `1`, `2` and `3`:

``` zsh
cd BoneyBank
dotnet run ../Configurations/configuration_sample.txt 1
dotnet run ../Configurations/configuration_sample.txt 2
dotnet run ../Configurations/configuration_sample.txt 3
```

Starting from the solution's directory, run 3 Bank Servers with id's `4`, `5`, `6`, `7`, `8`:

``` zsh
cd Bank
dotnet run ../Configurations/configuration_sample.txt 4
dotnet run ../Configurations/configuration_sample.txt 5
dotnet run ../Configurations/configuration_sample.txt 6
dotnet run ../Configurations/configuration_sample.txt 7
dotnet run ../Configurations/configuration_sample.txt 8
```

Starting from the solution's directory, run 3 Clients with id's `9`, `10` and `11`:

``` zsh
cd Clients
dotnet run ../Configurations/configuration_sample.txt 9
dotnet run ../Configurations/configuration_sample.txt 10
dotnet run ../Configurations/configuration_sample.txt 11
```
