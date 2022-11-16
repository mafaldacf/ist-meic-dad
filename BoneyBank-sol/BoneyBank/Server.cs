
using Grpc.Core;
using Timer = System.Timers.Timer;

namespace Boney;

public class Server{

    public enum Paxos_status{
        Propose,
        Prepare,
        Commit,
        Nothing,
        
    }
    
    private bool frozen = false;
    private Object frozen_lock = new Object();
    

    private Paxos_status status = Paxos_status.Nothing;
    private Object status_lock = new Object();
    
    private int read_ts = 1;
    private Object read_ts_lock = new Object();
    
    
    private int write_ts = 1;
    private Object write_ts_lock = new Object();


    private int replies = 0;
    private Object replies_lock = new Object();

    private int current_leader = 1;
    private Object current_leader_lock = new object();

    private PriorityQueue<int, int> queue = new PriorityQueue<int, int>();

    private int lastSlot = -1;
    private Dictionary<int, int> slots = new Dictionary<int, int>();

    private int bank_leader = 0;
    private Object bank_leader_lock = new Object();

    private List<PaxosClient> servers;

    private List<PrepareReply> prepareReplies = new List<PrepareReply>();

    private readonly int id;

    private Timer currentSlotTimer;


    private List<List<Tuple<string, string>>> slotInfo;

    /*
     * Handles the replies for both commit and propose requests
     * Ensures that the process only proceeds with a majority
     */
    private void replyHandler(int value,Paxos_status status,int slot){

        lock (status_lock){
            if (this.status != status) return; // replies arriving during a different state
        }

        lock (slots){ // ensuring we're handling the same slot
            if (this.lastSlot+1 != slot) return;

        }
        
        lock (replies_lock){
            if (value == -1)
                replies = -1;
            else{
                replies += replies == -1 ? 0 : 1;
            }
            Monitor.PulseAll(replies_lock);

        }
    }

    /**
     * Handles the prepare replies, by storing the replies
     */
    private void prepareHandler(PrepareReply reply){

        if (reply.Status == Status.Frozen) return;
        
        lock (status_lock){
            if (this.status != Paxos_status.Prepare) return; // propose replies in a different state
        }
        
        lock (slots){ // ensuring we're handling the same slot
            
            if(this.lastSlot+1 != reply.Slot) return;
            

        }
        
        lock (replies_lock){

            if (reply.Status == Status.Kill){
                replies = -1;
                return;
            }

            replies += replies == -1 ? 0 : 1;

            if (reply.Status == Status.Ok){
                lock (prepareReplies){
                    prepareReplies.Add(reply);
                }
            }
            
            Monitor.PulseAll(replies_lock);
        }
        
        
    }
    
    
    /*
     * Checks if a process is the leader
     *
     * Returns:
     * 1 -> process is the first leader
     * 2 -> process is the leader but must first prepare
     * 0 -> if process is not the leader
     */
    private int isLeader(){
        bool is_leader = false;
        bool new_leader = false;
        lock (current_leader_lock){
            int leader_id = (current_leader % (servers.Count + 1)) == 0
                ? servers.Count + 1
                : (current_leader % (servers.Count + 1));


            lock (slotInfo){ // while we suspect that the other leaders died, we keep incrementing the leader
                // eventually we will reach a leader that is not suspected or the process
                // will become leader
                lock (frozen_lock){
                    
                    while (slotInfo[0][leader_id-1].Item2  == "S"){

                        if (leader_id == this.id && !frozen) break;
                        
                        this.current_leader++;
                        new_leader = true;
                        leader_id = (current_leader % (servers.Count + 1)) == 0
                            ? servers.Count + 1
                            : (current_leader % (servers.Count + 1));
                    }
                    
                }
                
            }

            if (leader_id == this.id) is_leader = true;

            if(is_leader) return current_leader != 1 && new_leader ? 2 : 1;
            return 0;
        }
    }
    
    
    /**
     * Freezes the process, if it must be frozen.
     * Only when a new slot starts can a frozen process exit this function
     */
    private bool isFrozen(){
        lock (frozen_lock){
            return frozen;
        }

    }
    
    public void startTimer(){
        this.currentSlotTimer.Start();
    }
    

    public Server(List<Tuple<string,int>> servers,int id,int duration_slot,
        List<List<Tuple<string,string>>> slotInfo){
        this.servers = new List<PaxosClient>();
        foreach (Tuple<string,int> t in servers){
            this.servers.Add(new PaxosClient(t.Item1,t.Item2,replyHandler));
        }
        
        

        this.id = id;
        
        this.slotInfo = slotInfo;

        this.frozen = slotInfo[0][id - 1].Item1 == "N" ? false : true;
        this.currentSlotTimer = new Timer(duration_slot);
        currentSlotTimer.AutoReset = false;
        currentSlotTimer.Elapsed += (sender, e) => {
            Console.WriteLine("Starting a new slot");
            lock (slotInfo){


                this.slotInfo.RemoveAt(0); // change to next slot
                lock (frozen_lock){
                    
                    
                    if (this.slotInfo.Count == 0){ // no more slots
                        Console.WriteLine("There are no more slots left, ending the server");
                        System.Environment.Exit(0);
                    }
                    
                    // status on Item1 ; suspected Item2
                    if (this.slotInfo[0][id - 1].Item1 == "N")
                        this.frozen = false;
                    else{
                        this.frozen = true;
                    }
                    
                    
                }

                
            }
            
            
            this.currentSlotTimer.Start();
        };
        
        
        Thread c = new Thread(() => doConsensus());
            
        c.Start();






    }
    
    

    /**
     * Handles a commit request
     */
    public Status commit(CommitRequest request){
        if (isFrozen()) {
            throw new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "Frozen"));
        }

        lock (write_ts_lock){
            if (this.write_ts != request.WriteTs){
                Console.WriteLine("Received a commit order. Not Accepting it");
                return Status.Kill;
            }
        }
        
        lock (slots){
            slots[request.Slot] = request.Value;
            this.lastSlot = request.Slot;
            Monitor.PulseAll(slots);
            Console.WriteLine("Settled on bank leader {0} with write timestamp {1}",request.Value,request.WriteTs);
            
        }

       
        
        
        return Status.Ok;
    }
    
    /**
     * Handles the propose request
     */
    public Status propose(ProposeRequest request){

        if (isFrozen()) {
            throw new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "Frozen"));
        }

        lock (read_ts_lock){
            if (request.WriteTs != this.read_ts){
                Console.WriteLine("Received a propose. Not Accepting it");
                return Status.Kill;
            }
        }
        
        lock (write_ts_lock){
            Console.WriteLine("Received a propose from leader. Accepting it.");
            this.write_ts = request.WriteTs;
            this.bank_leader = request.Leader;
                
        }
        
        return Status.Ok;

    }
    
    
    /**
     * Handles a read request
     */
    public PrepareReply read(PrepareRequest request){
        if (isFrozen()) {
            throw new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "Frozen"));
        }


        Status s = Status.Ok;
        int leader = -1, write_ts = -1,local_read_ts;

        lock (read_ts_lock){
            if (request.ReadTs < this.read_ts){
                Console.WriteLine("Received a read request from the past");
                s  = Status.Kill;
            }
            else{
                this.read_ts = request.ReadTs;
            }

            local_read_ts = this.read_ts;
        }

        int local_current_leader; // just a lock work around and increase parallelism
        lock (current_leader_lock){
            this.current_leader = local_read_ts;
            local_current_leader = this.current_leader;

        }

        if (s != Status.Kill){ // ensure that I only reply to reads in the future 
            int previousSlot;
            lock (slots){
                previousSlot = this.lastSlot;
            }

            if (request.Slot > previousSlot){
                Console.WriteLine("Received a read request. But I don't have the value for the slot");
                s = Status.NotFound;
            }
            else{
                Console.WriteLine("Received a read request. Replied with <value,write_ts>");
                lock (write_ts_lock){
                    write_ts = this.write_ts;
                    
                }

                lock (bank_leader_lock){
                    leader = this.bank_leader;
                }
                
            }
            
        }
        
        return new PrepareReply{
            Leader = leader,
            Slot = request.Slot,
            Status = s,
            WriteTs = write_ts
        };
    }

    
    /**
     * Handles a compare and swap request
     */
    public CompareAndSwapReply start_consensus(int slot, int leader){
        
        Console.WriteLine("Compare and swap request for slot {0}!!",slot);
        if (isFrozen()) {
            throw new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "Frozen"));
        }

        bool start = false;
        lock (slots){ // if it has already been handled
            if (this.lastSlot >= slot){
                return new CompareAndSwapReply(){
                    Leader = slots[slot]
                };
            }
            
            if (!slots.ContainsKey(slot)){
                slots[slot] = -1;
                start = true;
            }
        }
        
        
        
        if (start){
            lock (queue){ // add to the consensus queue
                queue.Enqueue(leader,slot);
                Monitor.PulseAll(queue);
            }
        }

        lock (slots){
            // wait until the requested consensus is done
            while (this.lastSlot < slot ){
                Monitor.Wait(slots);
            }
            
            

            // since the requested consensus is done, reply with the answer
            return new CompareAndSwapReply(){
                Leader = slots[slot]
            };
            
            
        }
        
    }
    
    
    /**
     * Does all missing consensus in order. To ensure that all threads can reply
     * to their compare and swap requests
     */
    private void doConsensus(){
        
            while (true){

                int slot = 0, leader = 0,previousSlot;
                lock (queue){
                    while (queue.Count == 0) Monitor.Wait(queue);
                
                    Console.WriteLine("There are {0} consensus to be run",queue.Count);

                    lock (slots){ // just a lock work around to increase parallelism 
                        previousSlot = this.lastSlot;

                    }
                
                
                    queue.TryPeek(out leader, out slot); // check the top slot for consensus
                    while (slot != previousSlot+1){ // some consensus are missing, we will wait 
                        // all consensus will be done in order
                    
                        Console.WriteLine("This slot needs to wait for previous ones");
                        Monitor.Wait(queue);
                        queue.TryPeek(out leader, out slot);
                        
                        lock (slots){
                            previousSlot = this.lastSlot;
                        }
                    }
                    
                    queue.Dequeue();
                    
                }
                
                int l = paxos(slot, leader);

                if (l != -1){
                    lock (slots){
                        slots[slot] = l;
                        this.lastSlot = slot;
                        Monitor.PulseAll(slots); // wake up threads waiting for 
                    }
                }

            }

    }

   
    /**
     * Runs the classical paxos algorithm if the process is leader
     * If the process is not leader sets a timer to wait for the leader
     */
    private int paxos(int slot,int leader){
        
        Console.WriteLine("Starting Consensus...");

        

        int is_leader = isLeader();

        if (is_leader != 0){
            

            lock (replies_lock){
                this.replies = 0;

            }

            // prepare
            if (is_leader == 2){ // needs to prepare first
                
                int local_read_ts;
                lock (read_ts_lock){
                    this.read_ts = this.current_leader;
                    local_read_ts = this.read_ts;

                }
                
                
            
                lock (replies_lock){
                    
                    lock (status_lock){
                        status = Paxos_status.Prepare;

                    }

                    this.replies = 0;

                
                
                    Console.WriteLine("Preparing to become paxos leader");
                    foreach (PaxosClient p in servers){
                        Thread t = new Thread(() => p.read(new PrepareRequest{
                            ReadTs = local_read_ts,
                            Slot = slot
                        },prepareHandler));
                
                        t.Start();
                
                    }
                
                    Console.WriteLine("Waiting for a majority to proceed from prepare");

                    while (replies < (this.servers.Count+1) / 2 ){
                        Monitor.Wait(this.replies_lock);
                        if (replies == -1){
                            lock (prepareReplies){
                                this.prepareReplies.Clear();
                                this.replies = 0;
                                return -1;
                            }
                        }
                    }

                    this.replies = 0;
                }

                lock (prepareReplies){
                    
                    prepareReplies.Sort((x,y) => 
                        y.WriteTs.CompareTo(x.WriteTs));
                    
                    lock (bank_leader_lock){

                        this.bank_leader = prepareReplies.Count != 0 ? prepareReplies[0].Leader : leader;

                    }
                    
                }

                
            }
            else{ // first leader, doesn't need to prepare

                lock (bank_leader_lock){
                    this.bank_leader = leader;
                }
            }

            
            int local_write_ts;
            lock (write_ts_lock){
                this.write_ts = this.current_leader;
                local_write_ts = this.write_ts;
            }
            
            

            // propose
            lock (replies_lock){
                lock (status_lock){
                    status = Paxos_status.Propose;
                }
                
                this.replies = 0;
                
                
                Console.WriteLine("Proposing my value for all replicas");
                foreach (PaxosClient p in servers){
                    Thread t = new Thread(() => p.propose(new ProposeRequest{
                        Leader = leader,
                        WriteTs = local_write_ts,
                        Slot = slot
                    }));
                
                    t.Start();
                
                }
                
                while (replies < (this.servers.Count+1) / 2 ){
                    Monitor.Wait(this.replies_lock);
                    if (replies == -1){
                        this.replies = 0;
                        return -1; // no longer leader
                    }
                }
                
                this.replies = 0;
            }
            
            // commit
            
            lock (replies_lock){
                
                lock (status_lock){
                    status = Paxos_status.Commit;

                }

                this.replies = 0;
            
                Console.WriteLine("Giving the commit order...");
                foreach (PaxosClient p in servers){
                    Thread t = new Thread(() => p.commit(new CommitRequest{
                        Value = leader,
                        WriteTs = local_write_ts,
                        Slot = slot
                    }));
                
                    t.Start();
                
                }
                
                Console.WriteLine("Waiting for a majority to proceed from commit");

                while (replies < (this.servers.Count+1) / 2){
                    Monitor.Wait(this.replies_lock);
                    if (replies == -1){
                        this.replies = 0;
                        return -1; // no longer leader
                    }
                }
                
                Console.WriteLine("Paxos is done!");
                
                this.replies = 0;
            }
                
            lock (status_lock){
                status = Paxos_status.Nothing;

            }

            return leader;
        }
        return -1;

    }
    
    
    

}