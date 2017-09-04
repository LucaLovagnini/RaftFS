package PAD.RaftFS.Server;

import PAD.RaftFS.Utility.*;
import PAD.RaftFS.Utility.FSCommand.FSCommand;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.log4j.Logger;
import org.apache.mina.util.ConcurrentHashSet;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
/**
 * Created by luca-kun on 10/02/15.
 */

//TODO: implements snapshots and elasticity mechanisms

/**
 * This is the most complicate (and maybe important) class in the RaftFS project. It implements all the remote methods
 * that can be called on a server which belongs to the cluster, and its fields represents most of the server state.
 * There are essentially three categories of fields:
 * 1. Persistent fields: this is the most important category, since we have to guarantee that we keep track of them even
 *    if the server goes down. For this reason, each one (or a group) of them are written on a different file on the disk
 * 2. Volatile fields: this fields are used to implements the Raft algorithm or the FS features, but they can be reset
 *    every time that the server goes up.
 * 3. Utility and simulation fields: these fields are introduced for test, logging and simulation purpose, and they are
 *    totally unnecessary for the correct behavior of the entire system.
 */
public class ServerRMI implements ServerInterface{

    //-------------------------------------------------RAFT FIELDS-------------------------------------------------

    //persistent fields
    private int currentTerm,//the actual term of the server (Lamport's clock), initialized with 1
                lastApplied;//index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    private String votedFor;//identifier of the voted server by this server (if any, otherwise -1)
    private List<LogEntry> log;//list of committed and not-committed LogEntries replicated on this server

    //volatile fields on all servers
    public String   id, //the server id correspond to the host name in the network version,
                        // ServerI in the local one (where I is an integer>=0)
                    address;//the server ip address (127.0.0.1) in the local version
    String  leaderId = null,//used to redirect client requests
            leaderAddress = null;//used to redirect client requests
    int commitIndex = -1,//index of highest log entry known to be committed (initialized to 0, increases monotonically)
        lowerBoundElectionTimeout,
        randomElectionTimeout;
    boolean receivedAppendEntries = false,//if true, then if the candidate (or follower) becomes (remains) a follower
            grantedVote = false,//true if the server voted for someone else: the follower will not switch to candidate
            readOnly = false,//if true then the leader has committed an entry from this term and the first condition for readOnly is satisfied
            isLeader = false,//if false, then the leader cannot answer to read only operation
            isReady = false;//set to true when executor starts
    Executor executor;//used by checkTerm() in order to change the server state to Follower
    ReentrantLock lock = new ReentrantLock();
    //this lock is guarantee that each write operation on the log is atomic and thread-safe
    //(suppose that en entry is read while it's removed from the log!)
    ReadWriteLock logLock = new ReentrantReadWriteLock();
    StateMachineApplier stateMachineApplier;//this object performs the File System operations
    //----------------------------------------------SERVER FTP FIELDS--------------------------------------------------
    //this list is used as reference to each server which belongs to the cluster (see the class description for more details)
    //it's the only persistent field used to implement the FS
    private List<ClusterElement> cluster;
    //this collection keeps track of which nodes are reachable by the leader. it's used to verify the second read-only condition
    //(a leader in order to execute read-only operations must receive an heart beat answer from the majority of the servers)
    ConcurrentHashSet <String> reachableNodes = new ConcurrentHashSet<>();
    public GarbageCollector garbageCollector;//the garbageCollector guarantees that a partially uploaded file, or a file uploaded by mistake by an user is sooner or later deleted
    private int replicationFactor;//the replication factor represents on how many server each file is replicated by default. Its default value is 2 in both local and network versions
    private int networkSize;//the number of servers which belongs to the cluster
    //-----------------------------------------UTILITY AND SIMULATION FIELDS--------------------------------------------
    public enum DEBUG_LVL{silent,normal,debug,crazy}
    public DEBUG_LVL debugLvl;//depending of this value, the system will be more or less verbose
    Logger logger;//this logger was used to test how long the system remains without a leader: it writes on file when a leader is elected or is killed
    ServerKiller serverKiller = null;//this field was used for the same test, and it kills a leader (change its state to follower) after some time
    //the two fields below are used only for simulation purpose: the represent the probability to refuse to (respectively) send or receive a remote method
    private float   lossOutputProbability = 0,
                    lossInputProbability = 0;
    //this list too is used for simulation purpose, in particular to simulate network partitions
    List<String> blockedNodes = new ArrayList<>();

    public ServerRMI(Executor executor, int networkSize, int replicationFactor, ClusterElement myElement, List<ClusterElement> cluster, Logger logger, DEBUG_LVL debugLvl, int lowerBoundElectionTimeout, int randomElectionTimeout) throws ServerSetupException, UnknownHostException {
        this.logger = logger;
        this.lowerBoundElectionTimeout = lowerBoundElectionTimeout;
        this.randomElectionTimeout = randomElectionTimeout;
        this.id = myElement.getId();
        this.address = myElement.getAddress();
        this.networkSize = networkSize;
        this.replicationFactor = Math.min(replicationFactor,networkSize);
        this.debugLvl = debugLvl;
        this.lowerBoundElectionTimeout = lowerBoundElectionTimeout;
        this.randomElectionTimeout = randomElectionTimeout;
        this.executor = executor;
        setPersistentFields(cluster);
        setUpFTPServer(myElement.getPort());
    }

    /**
     * This method is called by the constructor and initializes the server persistent fields.
     * If this isn't the first time that the server join the cluster, then each file relative to the persistent fields must exist.
     * Otherwise it creates and initializes each file and relative field.
     * @param cluster a reference to the cluster list (not necessarily used)
     * @throws ServerSetupException
     */
    private void setPersistentFields(List<ClusterElement> cluster) throws ServerSetupException {
        if (Files.exists(Paths.get(id + "/metadata")) || //metadata, log and cluster files store ServerRMI fields
                Files.exists(Paths.get(id + "/log")) ||
                Files.exists(Paths.get(id + "/fileLocation")) || //fileLocation,fsStructure and scheduledOperations belongs to stateMachineApplier
                Files.exists(Paths.get(id + "/fsStructure")) ||
                Files.exists(Paths.get(id + "/scheduledOperations")) ||
                Files.exists(Paths.get(id + "/cluster"))
                ) {
            try (ObjectInputStream metadataFile = new ObjectInputStream(new FileInputStream(id + "/metadata"));
                 ObjectInputStream logFile = new ObjectInputStream(new FileInputStream(id + "/log"));
                 ObjectInputStream fileLocationFile = new ObjectInputStream(new FileInputStream(id + "/fileLocation"));
                 ObjectInputStream fsStructureFile = new ObjectInputStream(new FileInputStream(id + "/fsStructure"));
                 ObjectInputStream scheduledOperationsFile = new ObjectInputStream(new FileInputStream(id + "/scheduledOperations"));
                 ObjectInputStream clusterFile = new ObjectInputStream(new FileInputStream(id + "/cluster"))
            ) {
                currentTerm = metadataFile.readInt();
                lastApplied = metadataFile.readInt();
                votedFor = metadataFile.readUTF();
                log = (List<LogEntry>) logFile.readObject();
                stateMachineApplier = new StateMachineApplier(this,
                        (ConcurrentHashMap<String, Map.Entry<Long, List<ClusterElement>>>) fileLocationFile.readObject(),
                        (ConcurrentHashMap<String, ArrayList<String>>) fsStructureFile.readObject(),
                        (LinkedBlockingQueue<Map.Entry<FSCommand, Integer>>) scheduledOperationsFile.readObject());
                this.cluster = (List<ClusterElement>) clusterFile.readObject();
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
                throw new ServerSetupException("Error configuring " + id);
            }
        } else//if any configuration files don't exist then it's the first time that the server is used
        {
            File dir = new File(id);
            dir.mkdir();
            stateMachineApplier = new StateMachineApplier(this);
            setVotedFor("");
            setCurrentTerm(1);
            setLastApplied(-1);
            setLog(new ArrayList<LogEntry>());
            this.cluster = cluster;
            updateCluster();
        }
        stateMachineApplier.start();
    }

    /**
     * This method is called by the constructor above, and it initializes an Apache MINA FTPServer.
     * Each server contains only one user called "anonymous" with no password.
     * All the files contained in the FS are stored in an unique directory (which name is the same of the server name)
     * they are all stored on the same level, and they are uniquely identified by their paths, where each "/" is
     * substituted by a "@" char. So for example the /dir/file.jpg uploaded on the server hal9000 will be stored in ServerFS/hal9000/@dir@file.jpg
     * @param ftpServerPort the port where the FTPServer will listen to
     */
    //TODO: create some kind of user policy
    private void setUpFTPServer(int ftpServerPort) {
        try {
            //serversFS directory is created if multiple server are executed on the same host
            String homeName = "ServersFS/"+id+"FS";
            FtpServerFactory serverFactory = new FtpServerFactory();
            ListenerFactory factory = new ListenerFactory();
            // set the port of the listener
            factory.setPort(ftpServerPort);
            // replace the default listener
            serverFactory.addListener("default", factory.createListener());
            BaseUser user = new BaseUser();
            user.setName("anonymous");
            user.setPassword("");
            File homeDirectory = new File(homeName);
            if(!homeDirectory.exists())
                homeDirectory.mkdirs();
            user.setHomeDirectory(homeName);
            List<Authority> authorities = new ArrayList<>();
            authorities.add(new WritePermission());
            user.setAuthorities(authorities);
            UserManager userManager = serverFactory.getUserManager();
            userManager.save(user);
            // start the server
            FtpServer server = serverFactory.createServer();
            server.start();
            garbageCollector = new GarbageCollector(stateMachineApplier,homeName,id);
            ScheduledExecutorService garbageCollectorScheduler = Executors.newSingleThreadScheduledExecutor();
            garbageCollectorScheduler.scheduleAtFixedRate(garbageCollector,0,7, TimeUnit.MINUTES);
        } catch (FtpException e) {
            e.printStackTrace();
        }
    }

    public void setCurrentTerm(int currentTerm) {
        if(debugLvl.compareTo(DEBUG_LVL.debug)>=0)
            System.out.println(id+" term updated from "+this.currentTerm+" to "+currentTerm);
        this.currentTerm = currentTerm;
        updateMetadata();
    }

    public void setLastApplied(int lastApplied){
        if(lastApplied!=-1)
            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                System.out.println(id+" applied "+lastApplied);
        this.lastApplied = lastApplied;
        synchronized (this){
            this.notifyAll();
        }
        updateMetadata();
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        updateMetadata();
    }

    /**
     * This method is called whenever either currentTerm lastApplied or votedFor values are changed
     * and it writes their value on the metadata file
     */
    private void updateMetadata() {
        try(ObjectOutputStream metadataFile = new ObjectOutputStream(new FileOutputStream(id+"/metadata")))
        {
            metadataFile.writeInt(currentTerm);
            metadataFile.writeInt(lastApplied);
            metadataFile.writeUTF(votedFor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * If a cluster element is updated (for example its
     */
    public void updateCluster() {
        try(ObjectOutputStream clusterFile = new ObjectOutputStream(new FileOutputStream(id+"/cluster"))) {
            clusterFile.writeObject(cluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setLog(List<LogEntry> log) {
        logLock.writeLock().lock();
        this.log = log;
        updateLog();
        logLock.writeLock().unlock();
    }

    /**
     * This method is called when the leader append a single entry to a Follower's log.
     * During this method execution no one can read or write the log.
     * @param entry
     */
    public void addLog(LogEntry entry) {
        logLock.writeLock().lock();
        log.add(entry);
        updateLog();
        logLock.writeLock().unlock();
    }

    /**
     * This method is called in appendEntries when a conflict between the leader's log and the node's log has been found
     * It clears (delete) all the entries from startIndex to endIndex and update the log file
     */
    public void clearSubLog(int startIndex , int endIndex) {
        logLock.writeLock().lock();
        for(int i=startIndex;i<endIndex;i++)
            try {
                if(log.get(i).getFsCommand().getStub()!=null)//NoOperation has stub null!
                    log.get(i).getFsCommand().getStub().requestFailed(new RequestFailed("Operation failed because the log was overwritten, try again later!"));
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        log.subList(startIndex, endIndex).clear();
        updateLog();
        logLock.writeLock().unlock();
    }

    /**
     * This method is called in appendEntries after that clearSubLog has been called when a conflict between the
     * leader's log and the node's log has been found. It appends all the entries in list to the end of the node's log
     */
    public void addAllLog(List<LogEntry> list){
        logLock.writeLock().lock();
        log.addAll(list);
        updateLog();
        logLock.writeLock().unlock();
    }

    private void updateLog() {
        try(ObjectOutputStream metadataFile = new ObjectOutputStream(new FileOutputStream(id+"/log")))
        {
            metadataFile.writeObject(log);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateCommitIndex(int commitIndex) {
        List<Map.Entry<FSCommand,Integer>> commandsToBeScheduled = new ArrayList<>();
        for(int i=this.commitIndex+1;i<=commitIndex;i++)//+1 since commitIndex is the last committed entry (already passed to stateMachineApplier)
            commandsToBeScheduled.add(new AbstractMap.SimpleImmutableEntry<>(log.get(i).getFsCommand(),i));
        stateMachineApplier.scheduleCommands(commandsToBeScheduled);
        this.commitIndex = commitIndex;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public List<LogEntry> getLog() {
        return log;
    }

    public float getLossOutputProbability(){
        return lossOutputProbability;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public int getLastApplied() {return lastApplied;}

    public int lastLogIndex() {
        return log.size()-1;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public List <ClusterElement> getCluster() {
        return cluster;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public ConcurrentHashSet<String> getReachableNodes() {
        return reachableNodes;
    }

    public int getNetworkSize() {
        return networkSize;
    }

    public int lastLogTerm() {
        if(log.size()==0)
            return -1;
        else
            return log.get(log.size()-1).getTerm();
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public void setLossOutputProbability(float lossOutputProbability){
        if(lossOutputProbability >1|| lossOutputProbability <0)
            System.err.println("Invalid probability");
        else
            this.lossOutputProbability = lossOutputProbability;
    }

    public void setLossInputProbability(float lossInputProbability){
        if(lossOutputProbability >1|| lossOutputProbability <0)
            System.err.println("Invalid probability");
        else
            this.lossInputProbability = lossInputProbability;
    }

    /**
     * This method is the first instruction executed by each remote method and when a remote method answer is received
     * If term > currentTerm set currentTerm = term, and convert to follower
     * @param term
     */

    public boolean CheckTerm(int term)
    {
        if(term>currentTerm) {
            if(debugLvl.compareTo(DEBUG_LVL.debug)>=0)
                System.out.println(id+" update term from "+currentTerm+" to "+term);
            setCurrentTerm(term);
            executor.changeState(new Follower(this));
            return true;
        }
        else return term == currentTerm;

    }

    /**
     * This method is called by requestVote and checks if the candidate's log is updated at least as the follower's log or not
     * The candidate's log is up to date if the last entry term is greater than the term of the last follower's entry
     * or, if the terms of the last entries of the two logs are equals, then if the candidate's log is longer
     * otherwise, the follower's log is more recent than the candidate's one
     */
    private boolean LogUpToDate(int lastLogIndex , int lastLogTerm) {
        if(log.size()==0)//if log is empty, then any candidate's log it's obviously up-to-date
                return true;
        //if the candidate's last entry term is smaller than the follower's last entry term, then it's not updated
        if(lastLogTerm<log.get(log.size()-1).getTerm())
            return false;
        else
            //otherwise, if it's bigger then it's updated
            if(lastLogTerm > log.get(log.size()-1).getTerm())
                return true;//if the term of last candidate's entry is higher than actual node's term
            else//if they're equals, then the Candidate's log has to be at least long as the Follower's log
                return lastLogIndex >= log.size() - 1;
    }

    /**
     * This RPC is invoked by a candidate in order to request vote to this follower
     * @param term The candidate's term
     * @param candidateID The candidate's identifier
     * @param lastLogIndex The index of the last log entry of the candidate
     * @param lastLogTerm The term of the last log entry of the candidate
     * @return a new MethodAnswer object which contains the follower term and a boolean (true if success false otherwise)
     * @throws java.rmi.RemoteException
     */
    @Override
    public synchronized MethodAnswer requestVote(int term, String candidateID, int lastLogIndex, int lastLogTerm) throws RemoteException {
        if(debugLvl.compareTo(DEBUG_LVL.debug)>=0)
            System.out.println(id+" received requestVote from "+candidateID);
        if(Math.random()<lossInputProbability)//to simulate lost of input messages
            return new MethodAnswer(currentTerm,false);
        if(!isReady){
            if(debugLvl.compareTo(DEBUG_LVL.debug)>=0)
                System.out.println(id+" not ready yet!");
            return new MethodAnswer(currentTerm,false,false);
        }
        lock.lock();
        //if candidate's term is not updated or
        //if the follower has already voted for someone else or
        //if candidate's log is not updated
        //then follower cannot grant its vote
        if(CheckTerm(term) && (votedFor.equals("") || votedFor.equals(candidateID))  && LogUpToDate(lastLogIndex,lastLogTerm))
        {
            if(debugLvl.compareTo(DEBUG_LVL.debug)>=0)
                System.out.println(id+" voted for "+candidateID+" in term "+term);
            votedFor = candidateID;
            grantedVote = true;//now this Follower will not switch to Candidate when timeout will occurs
            lock.unlock();
            return new MethodAnswer(currentTerm,true);
        }
        else{
            if(debugLvl.compareTo(DEBUG_LVL.debug)>=0)
                System.out.println(id+" didn't vote "+candidateID+" for term "+term+" because votedFor="+votedFor+" currentTerm="+currentTerm+" logUpToDate="+LogUpToDate(lastLogIndex,lastLogTerm));
            lock.unlock();
            return new MethodAnswer(currentTerm,false);
        }
    }

    /**
     * Invoked by leader to add new entry logs to this follower or as simple heartbeat (empty entries list)
     * @param term leader's term
     * @param leaderID used to redirect clients requests
     * @param leaderAddress used to redirect clients request
     * @param prevLogIndex index of log entry immediately preceding new ones
     * @param prevLogTerm term of prevLogIndex entry
     * @param entries log entries to store
     * @param leaderCommit leader's commit index
     * @return currentTerm and success or not
     */
    @Override
    public MethodAnswer appendEntries(int term, String leaderID, String leaderAddress, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit, boolean isReadOnly) {
        if(debugLvl.compareTo(DEBUG_LVL.crazy)>=0)
            System.out.print("appendEntries Server"+id);
        if(Math.random()<lossInputProbability)//to simulate input messages loss
            return new MethodAnswer(currentTerm);
        if(!isReady) {
            if(debugLvl.compareTo(DEBUG_LVL.crazy)>=0)
                System.out.println(id+" not ready yet!");
            return new MethodAnswer(currentTerm, false,false);
        }
        lock.lock();
        if(!CheckTerm(term))//leader's term is too old
        {
            if(debugLvl.compareTo(DEBUG_LVL.crazy)>=0)
                System.out.println(id+" checkTerm failed: currentTerm="+currentTerm+" term="+term);
            lock.unlock();
            return new MethodAnswer(currentTerm,false);
        }

        //in order to perform a read-only operation we need that at least the majority of the nodes
        //answer to an heartbeat (see ServerRMI.clientRequest for more details)
        if(isReadOnly) {
            if(debugLvl.compareTo(DEBUG_LVL.crazy)>=0)
                System.out.println(id+" answering to heartBeat");
            lock.unlock();
            return new MethodAnswer(currentTerm, true);
        }
        if(debugLvl.compareTo(DEBUG_LVL.crazy)>=0)
            System.out.println("CheckTerm passed");
        receivedAppendEntries = true;//this appendEntries from this point will be valid (not necessarily successful)
        leaderId = leaderID;//so Follower can redirect clients
        this.leaderAddress = leaderAddress;
        //Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        //If prevLogIndex==-1 then nothing to check
        if(prevLogIndex!=-1&&(log.size()<=prevLogIndex||log.get(prevLogIndex).getTerm()!=prevLogTerm)) {
            lock.unlock();
            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                System.out.print(id+" prevLogIndex check failed: log.size()=" + log.size() + " prevLogIndex=" + prevLogIndex);
            if(log.size()>prevLogIndex)
                if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                    System.out.print(" log.get("+prevLogIndex+")="+log.get(prevLogIndex).getTerm()+" prevLogTerm="+prevLogTerm);
            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                System.out.println("");
            return new MethodAnswer(currentTerm, false);
        }

        int i;
        //If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
        for(i=0;i<entries.size();i++)
        {
            //+1 because prevLogIndex is the index immediately preceding the new ones
            if(prevLogIndex+1+i>=log.size())//if we have reached the end of follower's log, then stop
                break;
            if(log.get(prevLogIndex+1+i).getTerm()!=entries.get(i).getTerm())//conflict
            {
                if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                    System.out.println(id+" clear from index "+(prevLogIndex+1+i)+" to index "+log.size());
                clearSubLog(prevLogIndex + 1 + i, log.size());//delete the existing entry and all that follow it
                break;
            }
        }
        if(entries.size()-i>0)//if there are remaining entries
            addAllLog(entries.subList(i, entries.size()));//adds all the remaining entries
        //If leaderCommit > commitIndex, set commitIndex =
        //min(leaderCommit, index of last new entry)
        //example when commitIndex = index of last new entry: if the leader commits multiple entries while the node is
        //down, then the leader sends one (committed) entry per time and so last new entry < leaderCommit
        if(leaderCommit>commitIndex)
        {
            updateCommitIndex(Math.min(leaderCommit, log.size()-1));
        }
        if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
            if(entries.size()>0) {
                System.out.print("Log's print of " + id + ": ");
                for (i = 0; i < log.size(); i++)
                    System.out.print(log.get(i).getTerm() + " ");
                System.out.println("");
            }
        if(debugLvl.compareTo(DEBUG_LVL.crazy)>=0)
            System.out.println("Answering ok to append entry from "+leaderID);
        lock.unlock();
        return new MethodAnswer(currentTerm,true);
    }

    public void clientRequest(FSCommand command) throws RemoteException {
        if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
            System.out.println("Starting "+command);
        //if there is no leader now, try again later
        if(leaderId == null){
            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                System.out.println(id+" there is no leader now!");
            command.getStub().requestFailed(new RequestFailed("There is no leader now, try again later!"));
        }
        //it this node isn't the leader, then redirect the client to it
        else if(!leaderId.equals(id))
        {
            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                System.out.println(id+" the leader is "+leaderId);
            command.getStub().requestFailed(new RequestFailed(leaderId,leaderAddress, command));
        }
        else {
            //if command is a write command, append it to the log
            if (command.isWriteCommand()) {
                if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                    System.out.println("Executing write command");
                addLog(new LogEntry(currentTerm, command));
            }
            else {
                //otherwise check that the two condition in order to execute a read only operation are verified
                synchronized (this){
                    //if an entry from this term isn't submitted then goes to sleep until an entry is submitted (and then check it again)
                    while (!readOnly) {
                        if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                            System.out.println("command "+command+" going to sleep...");
                        try {
                            wait();
                            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                                System.out.println("Client "+Thread.currentThread()+" awaken!");
                        } catch (InterruptedException e) {
                            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                                System.out.println("Client "+Thread.currentThread()+" awaken!");
                        }
                        if(!isLeader) {//the Leader has become follower in the meanwhile: it's not safe to process the query
                            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                                System.out.println("Client "+Thread.currentThread()+" failed to process request");
                            command.getStub().requestFailed(new RequestFailed("command failed: "+id+" is not a Leader anymore!"));
                            return;
                        }
                    }
                }
                //it works as lower bound before the operation can be executed
                //after have verified the second condition, sleep until the machine applier have not applied the entry with index readIndex
                int readIndex = commitIndex;
                //Before process the query, we need to be sure that the leader is not a "fake leader"
                //(for example because of a partition), so the node sends an heartbeat to the majority of the nodes
                if(!heartBeatRound())
                {
                    command.getStub().requestFailed(new RequestFailed("command failed since the majority of the nodes are not reachable"));
                    return;
                }
                else
                    if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                        System.out.println("Received needed answers!");
                synchronized (this){
                    while (lastApplied<readIndex) {
                        if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                            System.out.println("command "+command+" going to sleep...");
                        try {
                            wait();
                            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                                System.out.println("Client "+Thread.currentThread()+" awaken!");
                        } catch (InterruptedException e) {
                            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                                System.out.println("Client "+Thread.currentThread()+" awaken!");
                        }
                    }
                }
                if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                    System.out.println("Processing query "+command);
                stateMachineApplier.executeReadCommand(command);//since a read operation is not written in the log, it doesn't matter the current term
            }
        }
    }

    /**
     * This method is invoked in order to be sure that the actual node (which should be a Leader) doesn't belong to
     * a partition with the minority of the nodes in the network. In other words, these methods checks if this
     * node is a "fake" Leader or not. In order to do so, it sends an heartbeat to all the nodes in the network and
     * check that it receives at least the majority of the answers.
     * It is invoked in two occasions: in ServerRMI.clientRequest() and in every implementation of FSCommand.executeCommand()
     * @return
     * @throws java.rmi.RemoteException
     */
    public boolean heartBeatRound() {
        int answered = 0;
        for(ClusterElement clusterElement : cluster)
        {
            /*notice that we cannot interrupt the round  as soon as we have collected the majority of the
            answers since we need to know which nodes are reachable (information used by putGetCandidates)
             */
            if(clusterElement.getId().equals(id))//don't heartbeat yourself ;)
                reachableNodes.add(id);
            else {
                String serverName = clusterElement.getId();
                try {
                    if (Math.random() >= getLossOutputProbability() && !blockedNodes.contains(serverName)) {
                        Registry registry = LocateRegistry.getRegistry(clusterElement.getAddress());
                        if(!Arrays.asList(registry.list()).contains(serverName))
                            continue;
                        ServerInterface stub = (ServerInterface) registry.lookup(serverName);
                        MethodAnswer answer = stub.appendEntries(currentTerm, id, address, -1, -1, new ArrayList<LogEntry>(), commitIndex, true);
                        reachableNodes.add(serverName);//if reach this line then the node is reachable somehow
                        if (!answer.isSuccess() || !isLeader) {
                            if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                                System.out.println("Client read operation failed because the node isn't the Leader anymore, try again later!");
                            return false;
                        } else
                            answered++;
                    }
                } catch (RemoteException e) {
                    reachableNodes.remove(serverName);
                    if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                        System.err.println(id + " can't reach " + serverName + " during heartBeatRound");
                } catch (NotBoundException e) {
                    reachableNodes.remove(serverName);
                    if(debugLvl.compareTo(DEBUG_LVL.normal)>=0)
                        System.err.println(id+" not bound "+serverName);
                }
            }
        }
        return answered >= networkSize / 2;
    }

}
