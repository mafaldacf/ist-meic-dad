using Timer = System.Timers.Timer;

namespace Bank {
    public class BankServer {

        private Dictionary<int, PrimaryBackupClient> bankServers;
        private Dictionary<int, BoneyService.BoneyServiceClient> boneyServers;
        private List<SortedDictionary<int, List<string>>> slotsInfo;

        private int slotDuration;
        private int currentSlot = 0;
        private Timer currentSlotTimer;

        private int id;
        private bool frozen = false;
        private Dictionary<int, int> receivedLeader;

        private List<Command> waitingCommands;
        private Dictionary<long, Command> tentativeCommands;
        private Dictionary<long, Command> commitedCommands;

        private int nrOfTentativeReplies = 0;
        private int numCleanupReplies = 0;
        private bool cleanup = false;
        private bool recover = false;

        private Object nrOfTentativeReplies_lock = new Object();
        private Object prepare_cleanup_replies_lock = new Object();
        private Object frozen_lock = new Object();
        private Object current_slot_lock = new Object();
        private Object cleanup_lock = new Object();
        private Object recover_lock = new Object();


        private List<ListPendingRequestsReply> cleanupReplies = new List<ListPendingRequestsReply>();

        private Thread ? t1;



        public BankServer(
            Dictionary<int, PrimaryBackupClient> bankServers,
            Dictionary<int, BoneyService.BoneyServiceClient> boneyServers,
            List<SortedDictionary<int, List<string>>> slotsInfo,
            int slotDuration,
            int id) {

            this.bankServers = bankServers;
            this.boneyServers = boneyServers;
            this.slotsInfo = slotsInfo;
            this.slotDuration = slotDuration;
            this.id = id;

            this.receivedLeader = new Dictionary<int, int>();
            this.waitingCommands = new List<Command>();
            this.tentativeCommands = new Dictionary<long, Command>();    
            this.commitedCommands = new Dictionary<long, Command>();


            this.frozen = slotsInfo[currentSlot][id][0] == "N" ? false : true;

            this.currentSlotTimer = new Timer(slotDuration);
            currentSlotTimer.AutoReset = false;
            currentSlotTimer.Elapsed += (sender, e) => {
                int slot;
                int leader;
                bool hasPreviousLeader;

                lock (current_slot_lock) {

                    // There are no more slots
                    if (currentSlot == this.slotsInfo.Count - 1) {
                        Console.WriteLine("There are no more slots, ending the server");
                        System.Environment.Exit(0);
                    }

                    // Next slot
                    currentSlot += 1;
                    slot = this.currentSlot;
                    Console.WriteLine("\n-------------------------------------------");
                    Console.WriteLine("Starting slot number: " + this.currentSlot);


                    lock (frozen_lock) {
                        if (slotsInfo[currentSlot][id][0] == "N") {
                            this.frozen = false;
                        }
                        else {
                            this.frozen = true;
                        }
                    }

                    this.currentSlotTimer.Start();

                    if (isFrozen()) {
                        Console.WriteLine("I am frozen now...");
                        return;
                    }
                }

                callBoneyToGetLeader(slot);

                lock (receivedLeader) {
                    leader = receivedLeader[slot];
                    hasPreviousLeader = receivedLeader.ContainsKey(slot-1);
                }

                // Wait if previous cleanup is not completed
                int prevSlot = slot > 0 ? slot - 1 : slot;
                if (isStillTheLeader(this.id, prevSlot)) {
                    lock (cleanup_lock) {
                        while (cleanup) {
                            Monitor.Wait(cleanup_lock);
                        }
                    }
                }

                lock (waitingCommands) { 
                    lock (tentativeCommands) { 
                        lock (commitedCommands) { 
                            Console.WriteLine($"waiting: {this.waitingCommands.Count}, " +
                                $"tentative {this.tentativeCommands.Count}, " +
                                $"commited {this.commitedCommands.Count}");
                        }
                    }
                }


                // Leader has changed and I am the new one -> start cleanup
                if (isNewLeader(slot)) {
                    startCleanup(slot);
                }

                // I was frozen last slot and leader is still the same => no cleanup
                if (!hasPreviousLeader && isStillTheLeader(leader, slot - 1)) {
                    // recover missing commits
                    recoverState(leader);
                }

                if(isLeader()) {
                    // Do 2-phase-commit protocol for waiting commands
                    Console.WriteLine("\nI am the leader, going to do 2PC.");
                    Thread t1 = new Thread(() => {
                        exec2PhaseCommit(slot);
                    });
                    t1.Start();
                }

            };
        }

        public void recoverState(int leader) {
            Command? command;
            PrimaryBackupClient client = bankServers[leader];
            long tentativeSequenceNumber;
            long lastSequenceNumber = calcNextSequenceNumber();
            if (lastSequenceNumber != -1) {
                lastSequenceNumber--;
            }


            RecoverStateReply reply = client.recoverState(new RecoverStateRequest {
                LastSequenceNumber = lastSequenceNumber
            });

            lock (recover_lock) {
                this.recover = true;
            }

            foreach (RecoveredCommit recoveredCommand in reply.RecoveredCommit) {
                Tuple<long, long> commandId = new Tuple<long, long>(recoveredCommand.Id[0], recoveredCommand.Id[1]);
                lock (waitingCommands) {

                    // command is already in tentative
                    lock (tentativeCommands) {
                        command = getTentativeCommand(commandId);
                        if (command != null) {
                            tentativeSequenceNumber = getTentativeSequenceNumber(commandId);
                            this.tentativeCommands.Remove(tentativeSequenceNumber);
                        }
                    }

                    // wait to receive command from the client
                    while (command == null) {
                        Monitor.Wait(waitingCommands);
                        command = getWaitingCommand(commandId);
                    }

                    waitingCommands.Remove(command);

                    lock (commitedCommands) {
                        Console.WriteLine($"Recovered and commited command {command}");
                        this.commitedCommands.Add(recoveredCommand.SequenceNumber, command);
                    }

                }

                lock (this) {
                    Monitor.PulseAll(this);
                }
                
            }

            lock (recover_lock) {
                this.recover = false;
                Monitor.PulseAll(recover_lock);
            }

        }

        public List<RecoveredCommit> doRecoverState(RecoverStateRequest request) {
            List<RecoveredCommit> recoveredCommits = new List<RecoveredCommit>();

            lock (recover_lock) {
                this.recover = true;
            }
            // lock current 2PC
            lock (this) {
                lock (commitedCommands) {
                    foreach (KeyValuePair<long, Command> command in commitedCommands) {
                        if (command.Key > request.LastSequenceNumber) {
                            recoveredCommits.Add(new RecoveredCommit {
                                Id = { command.Value.getId().Item1, command.Value.getId().Item2 },
                                SequenceNumber = command.Key,
                                Type = (int) command.Value.getType(),
                                Value = command.Value.getValue()
                            });
                        }
                    }
                }
            }
            lock (recover_lock) {
                this.recover = false;
                Monitor.PulseAll(recover_lock);
            }
            Console.WriteLine($"Received recover state and collected {recoveredCommits.Count}");
            return recoveredCommits;
        }


        public void startTimer() {
            int slot = this.currentSlot;
            Console.WriteLine("Starting slot number: 0");
            this.currentSlotTimer.Start();
            if (isFrozen()) {
                Console.WriteLine("I am frozen now...");
            }
            else {
                callBoneyToGetLeader(slot);
                this.currentSlotTimer.Start();

                if (isLeader()) {
                    Console.WriteLine("\nI am the leader, going to do 2-phase-commit.");
                    t1 = new Thread(() => {
                        exec2PhaseCommit(slot);
                    });
                    t1.Start();
                }
            }
        }

        public bool isFrozen() {
            lock(frozen_lock) {
                return this.frozen;
            }
        }

        public bool isLeader() {
            lock (current_slot_lock) {
                lock (receivedLeader) {
                    if (receivedLeader.ContainsKey(this.currentSlot)) {
                        return receivedLeader[this.currentSlot] == id;
                    }
                }
            }
            return false;
        }

        public bool isNewLeader(int slot) {
            int curr, prev;
            lock (receivedLeader) {
                curr = receivedLeader[slot];
                prev = receivedLeader.ContainsKey(slot-1) ? receivedLeader[slot-1] : -1;
            }

            if (curr == this.id && curr != prev) {
                return true;
            }

            return false;
        }

        public bool inCommited(ClientCredentials credentials) {
            lock (commitedCommands) {
                if (commitedCommands.Where(c => c.Value.getId().Item1 == credentials.Id &&
                    c.Value.getId().Item2 == credentials.SequenceNumber).ToList().Count > 0)
                    return true;
            }
            return false;
        }


        public bool isStillTheLeader(int leader, int initialSlot) {
            int slot;
            bool hasLeader = false;

            lock (current_slot_lock) {
                slot = this.currentSlot;
            }

            // wait to move to new slot
            lock (receivedLeader) {
                while (!receivedLeader.ContainsKey(slot)) {
                    Monitor.Wait(receivedLeader);
                }
            }

            while (initialSlot <= slot) {
                lock (receivedLeader) {
                    if (receivedLeader.ContainsKey(initialSlot)) {
                        if (receivedLeader[initialSlot] != leader) {
                            return false;
                        } else {
                            hasLeader = true;
                        }
                    } else {
                        hasLeader = false;
                    }
                }

                // was frozen, did not get leader
                if (!hasLeader) {
                    callBoneyToGetLeader(initialSlot);
                    lock (receivedLeader) {
                        while (!receivedLeader.ContainsKey(initialSlot)) {
                            Monitor.Wait(receivedLeader);
                        }
                    }
                    if (receivedLeader[initialSlot] != leader) {
                        return false;
                    }
                }
                initialSlot++;
            }
            return true;
        }

        public long calcNextSequenceNumber() {
            lock (commitedCommands) {
                lock (tentativeCommands) {
                    return Math.Max(this.commitedCommands.Keys.DefaultIfEmpty(-1).Max(), 
                                    this.tentativeCommands.Keys.DefaultIfEmpty(-1).Max()) + 1;
                }
            }
        }

        public bool isLastCommited(ClientCredentials credentials) {
            long sequenceNumber;
            lock (commitedCommands) {
                sequenceNumber = commitedCommands.Where(c => c.Value.getId().Item1 == credentials.Id &&
                    c.Value.getId().Item2 == credentials.SequenceNumber).FirstOrDefault().Key;
                if (sequenceNumber == default) return false;
                
            }
            return sequenceNumber == calcNextSequenceNumber() - 1;
        }

        public long getCommitedSequenceNumber(ClientCredentials credentials) {
            lock (commitedCommands) {
                long sequenceNumber = commitedCommands.Where(c => c.Value.getId().Item1 == credentials.Id &&
                    c.Value.getId().Item2 == credentials.SequenceNumber).FirstOrDefault().Key;
                if (sequenceNumber != default) {
                    return sequenceNumber;
                }
            }
            return -1;
        }
        public Command? getWaitingCommand(Tuple<long, long> commandId) {
            return waitingCommands
                .Where(c => c.getId().Item1 == commandId.Item1 
                && c.getId().Item2 == commandId.Item2).FirstOrDefault();
        }

        public Command? getTentativeCommand(Tuple<long, long> commandId)
        {
            KeyValuePair<long, Command> tentative = tentativeCommands
                .Where(c => c.Value.getId().Item1 == commandId.Item1 
                && c.Value.getId().Item2 == commandId.Item2).FirstOrDefault();

            if (tentative.Equals(default(KeyValuePair<long, Command>)))
                return null;

            return tentative.Value;
        }

        public long getTentativeSequenceNumber(Tuple<long, long> commandId) {
            KeyValuePair<long, Command> tentative = tentativeCommands
                .Where(c => c.Value.getId().Item1 == commandId.Item1
                && c.Value.getId().Item2 == commandId.Item2).FirstOrDefault();

            if (tentative.Equals(default(KeyValuePair<long, Command>)))
                return -1;

            return tentative.Key;
        }

        public void doCompareAndSwap(KeyValuePair<int, BoneyService.BoneyServiceClient> boney, int slot, int guess) {
                CompareAndSwapReply reply = boney.Value.compareAndSwap(new CompareAndSwapRequest {
                    Slot = slot,
                    Leader = guess
                });


            lock (receivedLeader) {
                    lock (current_slot_lock) {
                        receivedLeader.TryAdd(slot, reply.Leader);
                        Monitor.PulseAll(receivedLeader);
                    }
                }
                Console.WriteLine("CompareAndSwap request for slot " + slot + " for boney service #" + boney.Key + "\nReply: " + reply.Leader + "\n");
            }


        public void callBoneyToGetLeader(int slot) {

            int guess = guessLeader(slot);

            
            WaitHandle[] waitHandles = new WaitHandle[boneyServers.Count];
            int i = 0;

            foreach (var boney in boneyServers) {
                try {
                    var handle = new EventWaitHandle(false, EventResetMode.ManualReset);
                    Thread t = new Thread(() => {
                        doCompareAndSwap(boney, slot, guess);
                        handle.Set();
                    });
                    waitHandles[i++] = handle;
                    t.Start();

                } catch (Exception e) {
                    Console.WriteLine("Failed to contact boney server: " + e.Message);
                }
            }
            WaitHandle.WaitAny(waitHandles);
        }

        // The leader is the one with lower Id and not suspected
        public int guessLeader(int slot) {
            lock (receivedLeader) {

                // avoid unnecessary cleanups and vote for the previous leader if not suspected
                if (receivedLeader.ContainsKey(slot - 1)) {
                    int lastLeader = receivedLeader[slot - 1];
                    if (slotsInfo[slot][lastLeader][1] == "NS") {
                        return lastLeader;
                    }
                }
            }

            foreach (var bankServer in slotsInfo[slot].OrderBy(key => key.Key)) {
                if (slotsInfo[slot][bankServer.Key][1] == "NS")
                    return bankServer.Key;

                // votes for himself (not frozen) even if suspected by others
                else if (bankServer.Key == this.id && slotsInfo[slot][bankServer.Key][1] == "S")
                    return bankServer.Key;
            }
            return -1;
        }

        public bool doTentative(TentativeRequest request) {
            Command? command;
            Tuple<long, long> commandId = new Tuple<long, long>(request.CommandId[0], request.CommandId[1]);

            if (isStillTheLeader(request.Leader, request.Slot)) {

                // we are recovering state
                lock (recover_lock) {
                    while (recover) {
                        Monitor.Wait(recover);
                    }
                }

                // we are missing commits due to being frozen
                if (request.SequenceNumber > getLastKnownSequenceNumber() + 1) {
                    return false;
                }

                lock (commitedCommands) {

                    // command already commited in the past
                    if (commitedCommands.ContainsKey(request.SequenceNumber)) {
                        lock (this) {
                            Monitor.PulseAll(this);
                        }
                        return true;
                    }
                }

                lock (waitingCommands) {
                    command = getWaitingCommand(commandId);

                    // command is already in tentative
                    if (command == null) {
                        lock (tentativeCommands) {
                            command = getTentativeCommand(commandId);
                        }
                    }

                    // wait to receive command from the client
                    while (command == null) {
                        Monitor.Wait(waitingCommands);
                        command = getWaitingCommand(commandId);
                    }

                    // leader has changed while waiting for the request from the client
                    if (!isStillTheLeader(request.Leader, request.Slot)) {
                        return false;
                    }
                        
                    waitingCommands.Remove(command);
                }

                lock (tentativeCommands) {
                    // previous leader(s) proposed a sequence number but did not commit
                    List<KeyValuePair<long, Command>> previousTentatives = tentativeCommands
                        .Where (c => c.Value.getId().Item1 == commandId.Item1 && c.Value.getId().Item2 == commandId.Item2)
                        .ToList();
                    
                    foreach(KeyValuePair<long, Command> tentative in previousTentatives) {
                        tentativeCommands.Remove(tentative.Key);
                    }

                    tentativeCommands.Add(request.SequenceNumber, command);
                    Monitor.PulseAll(tentativeCommands);
                }

                Console.WriteLine($"Added tentative request {command} with sequence number {request.SequenceNumber} for leader {request.Leader}");
                return true;
            }

            return false;
        }

        public bool doCommit(CommitRequest request) {
            long seqNumber;
            Command command;

            if (isStillTheLeader(request.Leader, request.Slot)) {
                // we are missing commits due to being frozen
                if (request.SequenceNumber > getLastKnownSequenceNumber() + 1) {
                    return false;
                }

                lock (commitedCommands) {
                    // command already commited in the past
                    if (commitedCommands.ContainsKey(request.SequenceNumber)) {
                        lock (this) {
                            Monitor.PulseAll(this);
                        }
                        return true;
                    }

                    lock (tentativeCommands) {
                        // primary did not propose a sequence number yet
                        if (tentativeCommands.Count == 0) {
                            Monitor.Wait(tentativeCommands);
                        }

                        // leader has changed while waiting for the request from the client
                        if (!isStillTheLeader(request.Leader, request.Slot)) {
                            return false;
                        }

                        seqNumber = request.SequenceNumber;
                        command = tentativeCommands[seqNumber];
                        tentativeCommands.Remove(seqNumber);
                    }

                    commitedCommands.Add(seqNumber, command);

                }

                Console.WriteLine($"Commited request {command} with sequence number {seqNumber}");


                lock (this) {
                    Monitor.PulseAll(this);
                }
                return true;
            }

            return false;

        }

        public bool sendTentativeRequest(long seqNumber, Command command, int slot) {
            int leader;
            Console.WriteLine($"Sending tentative request to replicas for sequence number {seqNumber}, cleanup= {cleanup}, slot= {slot}");

            lock (receivedLeader) {
                leader = receivedLeader[slot];
            }

            lock (nrOfTentativeReplies_lock) {
                this.nrOfTentativeReplies = 0;

                // Send tentative requests
                foreach (PrimaryBackupClient c in bankServers.Values) {
                    Thread t = new Thread(() => {
                        c.tentative(new TentativeRequest {
                            SequenceNumber = seqNumber,
                            Slot = slot,
                            Leader = leader,
                            CommandId = { command.getId().Item1, command.getId().Item2 }
                        }, replyHandler);
                    });
                    t.Start();
                }

                // Wait for the majority of acknowledges
                while (this.nrOfTentativeReplies < (this.bankServers.Count + 1) / 2) {
                    Monitor.Wait(nrOfTentativeReplies_lock);
                    if (this.nrOfTentativeReplies == -1) {
                        this.nrOfTentativeReplies = 0;
                        return false; // no longer the primary
                    }
                }
                
                // Leader is doing cleanup and command is already commited
                lock (commitedCommands) {
                    if (commitedCommands.ContainsKey(seqNumber)) {
                        this.nrOfTentativeReplies = 0;
                        return true;
                    }
                }

                lock (tentativeCommands) {

                    // Leader is doing cleanup and command is already in tentative
                    if (!tentativeCommands.ContainsKey(seqNumber)) { 
                        tentativeCommands.Add(seqNumber, command);
                    }

                    lock (waitingCommands) {
                        waitingCommands.Remove(command);
                    }

                    lock (commitedCommands) {
                        commitedCommands.Add(seqNumber, command);
                    }

                    this.nrOfTentativeReplies = 0;
                    tentativeCommands.Remove(seqNumber);

                }
            }
            return true;
        }

        public void sendCommitRequest(long seqNumber, int slot) {
            int leader;
            Console.WriteLine("Sending commit request to replicas for sequence number " + seqNumber);

            lock (receivedLeader) {
                leader = receivedLeader[slot];
            }

            // Send commit requests
            foreach (PrimaryBackupClient c in bankServers.Values) {
                Thread t = new Thread(() => c.commit(new CommitRequest {
                    SequenceNumber = seqNumber,
                    Slot = slot,
                    Leader = leader,
                }));
                t.Start();
            }

            Monitor.PulseAll(this);

        }

        public bool inSlot(int slot) {
            int currentSlot;

            lock (current_slot_lock) {
                currentSlot = this.currentSlot;
            }
            return slot == this.currentSlot;
        }

        public void exec2PhaseCommit(int slot) {
            bool success = true;
            bool restart = false;
            while (inSlot(slot) && !isFrozen()) {
                
                Command command;
                long seqNumber;
                lock (waitingCommands) {
                    while (waitingCommands.Count == 0) {
                        Monitor.Wait(waitingCommands);
                    }

                    // For old threads to finish
                    if (!inSlot(slot) || isFrozen()) {
                        Console.WriteLine("Thread slot: " + slot + "; Current slot: " + this.currentSlot);
                        return;
                    }

                    // backup was recovering state and missed last commit
                    if (restart) {
                        lock (commitedCommands) {
                            long lastsequencenumber = getLastKnownSequenceNumber();
                            command = commitedCommands[lastsequencenumber];
                        }
                        restart = false;
                    } 
                    else {
                        command = waitingCommands[0];
                    }
                    command = waitingCommands[0];
                    Console.WriteLine("Started 2-phase-commit for command " + command);
                }


                lock (this) {
                    lock (recover_lock) {
                        seqNumber = calcNextSequenceNumber();

                        success = sendTentativeRequest(seqNumber, command, slot);

                        if (success) 
                            sendCommitRequest(seqNumber, slot);
                    }

                    // backup was recovering state and missed last commit
                    // recover won't commit while it doesn't acquire bank server lock
                    lock (recover_lock) {
                        if (recover) {
                            restart = true;
                        }
                    }
                }

                // wait until recover is done
                lock (recover_lock) {
                    while (recover) {
                        Monitor.Wait(recover_lock);
                    }
                }
            }            
        }

        public void addWaitingCommand(Command command) {
            lock (waitingCommands) {
                this.waitingCommands.Add(command);
                Monitor.PulseAll(waitingCommands);
            }

        }


        public void replyHandler(bool ack) {
            lock (nrOfTentativeReplies_lock) {
                if (ack) {
                    nrOfTentativeReplies += nrOfTentativeReplies == -1 ? 0 : 1;
                }
                // backups rejected tentative because primary is not the leader anymore
                else {
                    nrOfTentativeReplies = -1;
                }
                Monitor.PulseAll(nrOfTentativeReplies_lock);
            }
        }

        /**
         * Handles the cleanup replies and stores them
         */
        public void cleanupHandler(ListPendingRequestsReply reply) {
            lock (prepare_cleanup_replies_lock) {
                this.numCleanupReplies++;

                lock (cleanupReplies) {
                    this.cleanupReplies.Add(reply);
                }

                Monitor.PulseAll(prepare_cleanup_replies_lock);
            }
        }

        /**
         * Find command stored in waiting or commited requests with user id associated
         * If not found, return an instance of a new command object
         */
        public Command findCommand(CollectedRequest request) {
            Command? command;
            

            lock (waitingCommands) {
                command = waitingCommands.Find(c => c.getId().Item1 == request.Id[0] && c.getId().Item2 == request.Id[1]);
            }

            lock (commitedCommands) {
                if (command == null && commitedCommands.ContainsKey(request.SequenceNumber)) {
                    command = commitedCommands[request.SequenceNumber];
                }
            }

            if (command == null) command = RequestToCommandParser.parseCollectedRequest(request);

            return command;
        }

        /**
        * Verify if collected command from cleanup was already received from the client
        * and is waiting for a 2-Phase Commit
        */
        public bool isPendingCommand(Tuple<long, long> commandId) {
            lock (tentativeCommands) {
                if (tentativeCommands.Where(c => c.Value.getId().Item1 == commandId.Item1 &&
                    c.Value.getId().Item2 == commandId.Item2).ToList().Count > 0)
                    return true;
            }
            lock (commitedCommands) {
                if (commitedCommands.Where(c => c.Value.getId().Item1 == commandId.Item1 &&
                    c.Value.getId().Item2 == commandId.Item2).ToList().Count > 0)
                    return true;
            }
            return false;
        }

        public bool commandInCollected(SortedList<long, Command> collectedRequests, Command command) {
            return collectedRequests
                .Where(c => c.Value.getId().Item1 == command.getId().Item1
                && c.Value.getId().Item2 == command.getId().Item2).ToList().Count > 0;
        }

        public long getLastKnownSequenceNumber() {
            lock (commitedCommands) {
                return this.commitedCommands.Keys.DefaultIfEmpty(-1).Max();
            }
        }

        /**
        * New primary starts cleanup procedure.
        * Request a list of tentative and commands to backups.
        * Perform 2PC for commands not yet commited
        */
        public void startCleanup(int slot) {
            SortedList<long, Command> collectedRequests = new SortedList<long, Command>();
            Command? command;
            long lastKnownSequenceNumber;
            Dictionary<long, Command> commitedCommandsCopy;
            long minSequenceNumber = -1;
            int i = 0;

            lock (cleanup_lock) {
                this.cleanup = true;
            }

            lock (commitedCommands) {
                lastKnownSequenceNumber = getLastKnownSequenceNumber();
            }

            lock (prepare_cleanup_replies_lock) {
                this.numCleanupReplies = 0;
                this.cleanupReplies.Clear();
            }

            WaitHandle[] waitHandles = new WaitHandle[bankServers.Count];

            // Request a list of pending requests
            Console.WriteLine($"Starting cleanup procedure with last known sequence number {lastKnownSequenceNumber}");
            foreach (PrimaryBackupClient c in bankServers.Values) {
                var handle = new EventWaitHandle(false, EventResetMode.ManualReset);
                Thread t = new Thread(() => {
                    c.listPendingRequests(new ListPendingRequestsRequest {
                        LastKnownSequenceNumber = lastKnownSequenceNumber,
                        Slot = slot,
                        Leader = this.id
                    }, cleanupHandler);
                    handle.Set();
                });
                waitHandles[i++] = handle;
                t.Start();
            }

            WaitHandle.WaitAll(waitHandles, 200);

            lock (prepare_cleanup_replies_lock) {

                // Wait for majority of replies with collected sequence numbers
                while (numCleanupReplies < (this.bankServers.Count + 1) / 2) {
                    Monitor.Wait(prepare_cleanup_replies_lock);
                }

                // Get minimum sequence number known to be commited by frozen backups
                foreach (ListPendingRequestsReply reply in cleanupReplies) {
                    if (reply.LastSequenceNumber < minSequenceNumber) {
                        minSequenceNumber = reply.LastSequenceNumber;
                    }
                }

                // cannot lock bank server inside commited commands lock - receiving requests from clients
                lock (commitedCommands) {
                    commitedCommandsCopy = this.commitedCommands;
                }

                // Propose and Commit commands with no sequence number assigned from frozen backups
                foreach (KeyValuePair<long, Command> commitedCommand in commitedCommandsCopy) {
                    if (commitedCommand.Key > minSequenceNumber) {
                        lock (this) {
                            sendTentativeRequest(commitedCommand.Key, commitedCommand.Value, slot);
                            sendCommitRequest(commitedCommand.Key, slot);
                        }
                        minSequenceNumber++;
                    }
                }

                // Add (and sort) commited sequence numbers
                foreach (ListPendingRequestsReply reply in cleanupReplies) {
                    foreach (CollectedRequest request in reply.CommitedCommands) {
                        command = findCommand(request);

                        if (!collectedRequests.ContainsKey(request.SequenceNumber)) {
                            collectedRequests.Add(request.SequenceNumber, command);
                        }
                    }
                }

                // Add (and sort) tentative sequence numbers
                foreach (ListPendingRequestsReply reply in cleanupReplies) {
                    foreach (CollectedRequest request in reply.TentativeCommands) {
                        command = findCommand(request);

                        // Command not yet commited
                        // Otherwise, may have been commited (with diff number by new leader)
                        if (!commandInCollected(collectedRequests, command)) {

                            // Command not yet commited with associated key
                            if (!collectedRequests.ContainsKey(request.SequenceNumber)) {
                                collectedRequests.Add(request.SequenceNumber, command);
                            }
                        }
                    }
                }

                // Add (and sort) own tentative sequence numbers
                lock (tentativeCommands) {
                    foreach (KeyValuePair<long, Command> request in tentativeCommands) {

                        // Command not yet commited
                        // Otherwise, may have been commited (with diff number by new leader)
                        if (!commandInCollected(collectedRequests, request.Value)) {

                            // Command not yet commited with associated key
                            if (!collectedRequests.ContainsKey(request.Key)) {
                                collectedRequests.Add(request.Key, request.Value);
                            }
                        }
                    }
                }

                // Propose and commit ordered collected sequence numbers
                foreach (KeyValuePair<long, Command> request in collectedRequests) {
                    command = request.Value;

                    if (!isPendingCommand(request.Value.getId())) {
                        // Did not receive command from the client yet: ensure order of client responses
                        lock (waitingCommands) {
                            command = getWaitingCommand(request.Value.getId());


                            while (command == null) {
                                Monitor.Wait(waitingCommands);
                                command = getWaitingCommand(request.Value.getId());

                            }

                        }

                    }

                    lock (this) {
                        sendTentativeRequest(request.Key, command, slot);
                        sendCommitRequest(request.Key, slot);
                    }
                }


                this.numCleanupReplies = 0;
                this.cleanupReplies.Clear();
            }

            lock (cleanup_lock) {
                this.cleanup = false;
                Monitor.PulseAll(cleanup_lock);
            }

            Console.WriteLine($"Cleanup is done for {collectedRequests.Count} collected requests.");
        }



        /**
        * Backups returns a list of tentative and commited commands to the new primary with 
        * sequence number higher than the last known sequence number commited on primary.
        */
        public Tuple<List<List<CollectedRequest>>, long> listPendingRequests(long lastKnownSequenceNumber, int slot, int leader) {
            List<CollectedRequest> collectedTentativeCommands = new List<CollectedRequest>();
            List<CollectedRequest> collectedCommitedCommands = new List<CollectedRequest>();
            List<List<CollectedRequest>> collectedPendingCommands = new List<List<CollectedRequest>>();
            int numCollected = 0;
            long lastSequenceNumber = calcNextSequenceNumber();

            if (lastSequenceNumber != -1) {
                lastSequenceNumber--;
            }

            int currLeader, currSlot;

            lock (current_slot_lock) {
                currSlot = this.currentSlot;
            }

            // Received a request from the past
            if (currSlot > slot) {
                collectedPendingCommands.Add(collectedCommitedCommands);
                collectedPendingCommands.Add(collectedTentativeCommands);
                new Tuple<List<List<CollectedRequest>>, long>(collectedPendingCommands, lastSequenceNumber);
            }

            // Wait until we move to the next slot and get the corresponding leader
            lock (receivedLeader) {
                while (!receivedLeader.ContainsKey(currSlot)) {
                    Monitor.Wait(receivedLeader);
                }
                currLeader = receivedLeader[currSlot];
            }

            // Collect commited commands
            lock (this.commitedCommands) {
                foreach (KeyValuePair<long, Command> command in this.commitedCommands) {

                    // only collect requests the new primary does not have
                    if (command.Key > lastKnownSequenceNumber) {
                        CollectedRequest commitedCommand = new CollectedRequest {
                            SequenceNumber = command.Key,
                            Id = { command.Value.getId().Item1, command.Value.getId().Item2 }
                        };
                        collectedCommitedCommands.Add(commitedCommand);
                        numCollected++;
                    }
                }
            }

            // Collect tentative commands
            lock (this.tentativeCommands) {
                foreach (KeyValuePair<long, Command> command in this.tentativeCommands) {

                    CollectedRequest tentativeCommand = new CollectedRequest {
                        SequenceNumber = command.Key,
                        Id = { command.Value.getId().Item1, command.Value.getId().Item2 }
                    };
                    collectedTentativeCommands.Add(tentativeCommand);
                    numCollected++;
                }
            }

            collectedPendingCommands.Add(collectedCommitedCommands);
            collectedPendingCommands.Add(collectedTentativeCommands);


            Console.WriteLine($"Received cleanup request and collected {numCollected} requests");
            return new Tuple<List<List<CollectedRequest>>, long>(collectedPendingCommands, lastSequenceNumber);
        }
    }
}
