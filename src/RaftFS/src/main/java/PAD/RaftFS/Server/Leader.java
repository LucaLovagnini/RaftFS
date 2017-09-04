package PAD.RaftFS.Server;

import PAD.RaftFS.Utility.ClusterElement;
import PAD.RaftFS.Utility.FSCommand.NoOperation;
import PAD.RaftFS.Utility.FSCommand.PathFormatException;
import PAD.RaftFS.Utility.LogEntry;
import PAD.RaftFS.Utility.MethodAnswer;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by luca on 16/02/15.
 */
public class Leader extends ServerState {

    //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    HashMap<String,Integer> nextIndex;
    //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    HashMap<String,Integer> matchIndex;

    /**
     * This constructor is used when a node becomes leader for the first time (when a candidates win the election)
     */
    public Leader(ServerRMI serverRMI)
    {
        super(serverRMI);
        serverRMI.leaderId = serverRMI.id;
        nextIndex = new HashMap<>();//initialized when a nodes becomes a leader (both updated with first appendEntries)
        matchIndex = new HashMap<>();
        alreadyLeader = false;//first time as leader (no timeout has elapsed)
        serverRMI.isLeader = true;
        serverRMI.readOnly = false;//when a node becomes leader for the first time, it cannot answer to read only operations
        for(ClusterElement clusterElement : serverRMI.getCluster())
        {
            if (clusterElement.getId().equals(serverRMI.id))//next and match index are defined only on other nodes
                continue;
            nextIndex.put(clusterElement.getId(), serverRMI.getLog().size());//size = leader last log index +1
            matchIndex.put(clusterElement.getId(), -1);//starting with no matching
        }
    }

    /**
     * This constructor is used when the leader's timeout elapses, and so resubmit the same task (in other words stay a Leader)
     * It has to "remember" the old values (matchIndex and nextIndex)
     * @param matchIndex
     * @param nextIndex
     */
    public Leader(ServerRMI serverRMI , HashMap<String,Integer> matchIndex , HashMap<String,Integer> nextIndex) {
        super(serverRMI);
        serverRMI.leaderId = serverRMI.id;
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
        this.alreadyLeader = true;
        serverRMI.isLeader = true;
    }



    @Override
    public void run() {
        if(!alreadyLeader) {
            serverRMI.logger.info(serverRMI.id+" is a leader,"+serverRMI.getCurrentTerm());
            //the serverKiller is a daemon thread which force a leader to become a server after some time
            //it was used only to test the amount of time where the system didn't have a leader
            //the author left these codes (except for the start instruction of course) for completeness
            if(serverRMI.serverKiller!=null)
                serverRMI.serverKiller.interrupt();
            serverRMI.serverKiller = new ServerKiller(serverRMI.executor,serverRMI);
            //serverRMI.serverKiller.start();
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                System.out.println(serverRMI.id+" is the leader of term "+serverRMI.getCurrentTerm());
            //Upon becoming leader, append no-entry to log
            //A no-entry operation does NOTHING, but it's written in the log anyway (first read-only condition)
            try {
                serverRMI.addLog(new LogEntry(serverRMI.getCurrentTerm() , new NoOperation()));
            } catch (PathFormatException e) {//never thrown
                e.printStackTrace();
            }
        }
        boolean isInterrupted = false;
        int size = serverRMI.getLog().size();
        int myTerm = serverRMI.getCurrentTerm();
        //until a leader isn't interrupted (back to follower or timeout elapses) keep sending appendEntries
        while (!isInterrupted && !(isInterrupted = Thread.currentThread().isInterrupted()))
        {
            //TODO: sends appendEntries in parallel (without adding too much overhead)
            for(ClusterElement clusterElement : serverRMI.getCluster()) {//for each server in the cluster
                try {
                    if(isInterrupted || (isInterrupted = Thread.currentThread().isInterrupted()))
                        break;
                    if(clusterElement.getId().equals(serverRMI.id))//don't append entries to yourself ;)
                        continue;
                    String elementId = clusterElement.getId();
                    Registry registry = LocateRegistry.getRegistry(clusterElement.getAddress());
                    ServerInterface stub = (ServerInterface) registry.lookup(elementId);
                    List<LogEntry> entries = new ArrayList<>();
                    //Two cases when entries are sent:
                    //1. There is some new entry
                    //2. Some entry is not yet replicated
                    //Example 2. : leader receives 2 entries->leader goes immediately down->leader resume:
                    //the last two (not yet replicated) entries are sent
                    if (nextIndex.get(elementId) <= serverRMI.getLog().size() -1) {
                        //while we read the log we have to be sure that nobody is modifying it in the meantime
                        //for instance the true leader delete all the entries that we are reading (concurrent exception)
                        serverRMI.logLock.readLock().lock();
                        entries.addAll(serverRMI.getLog().subList(nextIndex.get(elementId),serverRMI.getLog().size()));
                        serverRMI.logLock.readLock().unlock();
                    }
                    // MethodAnswer appendEntries(int term , int leaderID , int prevLogIndex , int prevLogTerm , List<LogEntry> entries , int leaderCommit)
                    //else: send an heartbeat (entries is empty)
                    if(Math.random()>=serverRMI.getLossOutputProbability() && !serverRMI.blockedNodes.contains(elementId)) {
                        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                            System.out.println(serverRMI.id+" sending appendEntries to "+serverRMI.getCurrentTerm());
                        MethodAnswer answer;
                        if (nextIndex.get(elementId) == 0)//if it's the first replication ( serverRMI.log.get(nextIndex.get(i) - 1).getTerm() would raise an ArrayIndexOutOfBoundsException )
                        {
                            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                                System.out.println(serverRMI.id+"\treplicating first entry");
                            answer = stub.appendEntries(myTerm, serverRMI.id, serverRMI.address, -1, -1, entries, serverRMI.getCommitIndex(), false);
                        } else {
                            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0) {
                                System.out.println("\tprevLogIndex(" + elementId + ")=" + (nextIndex.get(elementId) - 1));
                                System.out.println("\tprevLogTerm(" + elementId + ")=" + serverRMI.getLog().get(nextIndex.get(elementId) - 1).getTerm());
                            }
                            answer = stub.appendEntries(myTerm, serverRMI.id, serverRMI.address, nextIndex.get(elementId) - 1, serverRMI.getLog().get(nextIndex.get(elementId) - 1).getTerm(), entries, serverRMI.getCommitIndex(), false);
                        }
                        /*This check is important for network partition. During a network partition there could be 2 leaders. When the partition is healed, the "real" leader
                        (the one with the higher term) could contacted the "fake" leader (this on) through an appendEntries, and so updated its term through CheckTerm() and turn
                        it back to a follower (interrupting this thread). As conclusion, if we would not make this check, then next if condition would result false and go on until
                        the next cycle (modifying nextIndex in a wrong way)
                         */
                        if(isInterrupted = Thread.currentThread().isInterrupted())
                            continue;
                        if (answer.getTerm() > serverRMI.getCurrentTerm())//the Leader is "old" (there are newer candidates or Leader)
                        {
                            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                                System.out.println(serverRMI.id+" leader of term "+myTerm+" found "+elementId+" with term "+answer.getTerm());
                            if(serverRMI.serverKiller!=null)
                                serverRMI.serverKiller.interrupt();
                            serverRMI.setCurrentTerm(answer.getTerm());
                            return;
                        }
                        if (answer.isSuccess()){
                            matchIndex.put(elementId, nextIndex.get(elementId)-1+entries.size());//updates matchIndex
                            if(nextIndex.get(elementId)<=matchIndex.get(elementId)) {//update nextIndex
                                if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                                    System.out.println("\t nextIndex("+elementId+") updated from "+nextIndex.get(elementId)+" to "+(matchIndex.get(elementId)+1));
                                nextIndex.put(elementId, matchIndex.get(elementId) + 1);
                            }
                        } else {
                            if(!answer.isReady()){
                                if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                                    System.out.println(serverRMI.id+" server "+clusterElement.getId()+" is not ready yet!");
                            }else
                            if(!answer.isLost())
                            {
                                //if the answer isn't lost and it's not successful, then proceed with back-rolling: decrement nextIndex
                                if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                                    System.out.println(serverRMI.id + " Decrement nextIndex(" + elementId + ") to " + (nextIndex.get(elementId) - 1));
                                nextIndex.put(elementId, nextIndex.get(elementId) - 1);//decrement next index and retry
                            }
                            else
                            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                                System.out.println(serverRMI.id+" answer lost from "+elementId);
                        }
                    }
                }
                catch (NoSuchObjectException | java.rmi.ConnectException | java.rmi.ConnectIOException e){
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                        System.err.println("Leader "+serverRMI.id+" cannot append entry to "+clusterElement.getId()+" because not reachable ");
                } catch (UnmarshalException e) {
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                        System.err.println("Leader " + serverRMI.id + " timeout appending entry to " + clusterElement.getId());
                }
                catch (RemoteException e) {
                    e.printStackTrace();
                } catch (NotBoundException e) {
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                        System.err.println("Leader "+serverRMI.id+" not bound "+clusterElement.getId());
                }
            }
            //new entries from last cycle, print them
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0) {
                if (size < serverRMI.getLog().size()) {
                    size = serverRMI.getLog().size();
                    System.out.print(serverRMI.id + " Log's print: ");
                    for (int i = 0; i < serverRMI.getLog().size(); i++)
                        System.out.print(serverRMI.getLog().get(i).getTerm() + " ");
                    System.out.println();
                }
            }
            if(serverRMI.getCommitIndex()<serverRMI.getLog().size()-1)//if there is still some not committed entry
            {
                for(int i=serverRMI.getLog().size()-1;i>serverRMI.getCommitIndex();i--)//if there exists an N such that N > commitIndex...
                {
                    if(serverRMI.getLog().get(i).getTerm()==serverRMI.getCurrentTerm())//such that log[N].term == currentTerm
                    {
                        //and such that there exists majority of matchIndex[i] ≥ N...
                        int numberOfReplica = 1;
                        for (int match : matchIndex.values()) {
                            numberOfReplica = match >= i ? numberOfReplica + 1 : numberOfReplica;
                            if(numberOfReplica>(serverRMI.getNetworkSize())/2)
                                break;
                        }
                        if(numberOfReplica>(serverRMI.getNetworkSize())/2) {
                            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                                System.out.println(serverRMI.id + " committing index " + i);
                            serverRMI.updateCommitIndex(i);//then commit all entries up to N
                            if(serverRMI.getLog().get(i).getTerm()==serverRMI.getCurrentTerm()) {
                                if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                                    System.out.println(serverRMI.id+" committed an actual entry!");
                                synchronized (serverRMI) {
                                    serverRMI.readOnly = true;
                                    serverRMI.notifyAll();//wake up all waiting read-only requests (first condition verified)
                                }
                            }
                            break;
                        }
                    }
                    else
                        /*since the value of each log's entry increases monotonically, if log[N].term != currentTerm
                        (and so it's smaller), then for each M s.t. M<N, then log[M] != currentTerm, and so we can stop
                        from searching an actual entry to commit*/
                        break;
                }
            }
        }

    }

    @Override
    public ServerState timeout() {
        return new Leader(serverRMI, matchIndex, nextIndex);//leader has to remember the matchedIndex and nextIndex
    }

    @Override
    public ServerState taskCompleted() throws UnexpectedStateException{
        synchronized (serverRMI) {
            serverRMI.readOnly = false;
            serverRMI.isLeader = false;
            serverRMI.notifyAll();//notify all pending reads that this node isn't the leader anymore
        }
        return new Follower(serverRMI);//since it comes back to a follower, it forgets who voted before (itself)
    }

    @Override
    public long generateTimeout() {
        return timeoutGenerator.nextInt(10000);//timeout isn't important for the leader
    }
}
