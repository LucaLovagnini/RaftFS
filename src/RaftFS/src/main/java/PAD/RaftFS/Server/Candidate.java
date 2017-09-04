package PAD.RaftFS.Server;

import PAD.RaftFS.Utility.ClusterElement;
import PAD.RaftFS.Utility.MethodAnswer;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

/**
 * Created by luca on 16/02/15.
 */
public class Candidate extends ServerState {

    ServerState nextState;

    public Candidate(ServerRMI serverRMI) {
        super(serverRMI);
        /*On conversion to candidate, start election:
        1. Increment currentTerm
        2. Vote for itself*/
        alreadyLeader = false;
        serverRMI.setCurrentTerm(serverRMI.getCurrentTerm() + 1);
        serverRMI.setVotedFor(serverRMI.id);
        serverRMI.leaderId = null;
        nextState=null;
    }

    @Override
    public void run() {
        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
            System.out.println(serverRMI.id + " is a candidate of term " + serverRMI.getCurrentTerm());
        int votes = 1;//number of gained votes, each node votes for itself
        int electionTerm = serverRMI.getCurrentTerm();
        boolean isInterrupted = false;//this variable is necessary since testing isInterrupted() reset the flag (and we need to test it twice)
        //Send requestVote RPCs to all other server
        //A Node's thread could be interrupted if:
        // 1. it receives a RPC from a node with an higher term
        // 2. a valid appendEntries from the Leader
        //If it invokes a RPC of an another node and the term in the answer is higher than its own term, then it switch to follower by itself
        for(ClusterElement clusterElement : serverRMI.getCluster()) {
            if (isInterrupted || (isInterrupted = Thread.currentThread().isInterrupted()) || votes > serverRMI.getNetworkSize() / 2)
                break;
            if (clusterElement.getId().equals(serverRMI.id))//don't ask vote to yourself ;)
                continue;
            //this check is used only in order to simulate packet loss and network partitions
            if (Math.random() >= serverRMI.getLossOutputProbability() && !serverRMI.blockedNodes.contains(clusterElement.getId())) {
                try {
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                        System.out.println(serverRMI.id+" asking vote to "+clusterElement.getId());
                    Registry registry = LocateRegistry.getRegistry(clusterElement.getAddress());
                    //TODO: implement a different (more efficient) mechanism where the (expensive) lookup operation is not performed every time
                    ServerInterface stub = (ServerInterface) registry.lookup(clusterElement.getId());//get
                                //MethodAnswer requestVote (int term , int serverRMI.id , int lastLogIndex , int lastLogTerm)
                    MethodAnswer answer = stub.requestVote(electionTerm, serverRMI.id, serverRMI.lastLogIndex(), serverRMI.lastLogTerm());
                    //if there is a newer candidate/leader
                    if (answer.getTerm() > serverRMI.getCurrentTerm()) {
                        nextState = new Follower(serverRMI);
                        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                            System.out.println(serverRMI.id+" leaving election of term "+serverRMI.getCurrentTerm()+" because found Server"+clusterElement.getId()+" with term "+answer.getTerm());
                        serverRMI.setCurrentTerm(answer.getTerm());
                        break;
                    } else if (answer.isSuccess()) {//if true then the Follower granted its vote to this Candidate
                        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                            System.out.println(serverRMI.id+" get vote from "+clusterElement.getId());
                        votes++;
                    }
                    else {
                        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                            System.out.println(serverRMI.id+" didn't get vote from "+clusterElement.getId());
                    }
                    if (votes > (serverRMI.getNetworkSize()) / 2)
                        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                        System.out.println(serverRMI.id + " reached quorum with " + votes + " votes");
                } catch (NoSuchObjectException | java.rmi.ConnectException | java.rmi.ConnectIOException e){
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                        System.err.println("Candidate "+serverRMI.id+" cannot request vote to "+clusterElement.getId()+" because not reachable");
                } catch (UnmarshalException e) {
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                        System.err.println("Candidate " + serverRMI.id + " timeout requesting vote to " + clusterElement.getId());
                } catch (RemoteException e) {
                    e.printStackTrace();
                } catch (NotBoundException e) {
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                        System.out.println("Candidate "+serverRMI.id+" not bound "+clusterElement.getId());
                }
            } else {
                if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                    System.out.println(serverRMI.id + " failed to send requestVote to " + clusterElement.getId());
            }
        }
        /*If the node was not converted to a Follower (so the Thread was not interrupted) and it's still a possible
        candidate (so he didn't find a Node with an higher term and nextState is null), then check if it got the majority of the votes*/
        if (!isInterrupted && !Thread.currentThread().isInterrupted() && nextState == null)
            if (votes > (serverRMI.getNetworkSize()) / 2) {
                if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                    System.out.println(serverRMI.id+" Election term "+serverRMI.getCurrentTerm()+" succeeded with "+votes+" votes");
                nextState = new Leader(serverRMI);
            } else {
                if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                    System.out.println(serverRMI.id + " Election term " + serverRMI.getCurrentTerm() + " failed with " + votes + " votes");
                //if election fails, sleep until timeout or until interruption (caused by changing state as follower)
                try {
                    if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                        System.out.println(serverRMI.id+" goes to sleep after have lost the election...");
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    return;
                }
            }

    }

    @Override
    public ServerState timeout() {
        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
            System.out.println(serverRMI.id+" candidate timeout!");
        if(serverRMI.receivedAppendEntries){
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                System.out.println(serverRMI.id+" back to follower!");
            return new Follower(serverRMI, serverRMI.getVotedFor());}
        else {
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                System.out.println(serverRMI.id+" starts a new election!");
            return new Candidate(serverRMI);
        }
    }

    @Override
    public ServerState taskCompleted() throws UnexpectedStateException{
        /*if the Candidate terminated run(), then it could become:
        A Follower if a requestVote returned a term bigger than serverRMI.currentTerm
        A Leader if the majority of the Servers voted for the Candidate
         */
        if(nextState == null)
            throw new UnexpectedStateException();
        return nextState;
    }

    @Override
    public long generateTimeout() {
        return timeoutGenerator.nextInt(serverRMI.randomElectionTimeout)+serverRMI.lowerBoundElectionTimeout;
    }
}
