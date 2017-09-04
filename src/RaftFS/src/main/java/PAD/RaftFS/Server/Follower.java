package PAD.RaftFS.Server;

public class Follower extends ServerState{

    /**
     * This constructor is used when:
     * 1. As first state for a node
     * 2. When a candidate becomes a Follower
     * 3. When a leader becomes a Follower
     * 4. The node has been contacted by a node with an higher term
     */
    public Follower(ServerRMI serverRMI)
    {
        super(serverRMI);
        alreadyLeader = false;
        serverRMI.grantedVote = false;
        serverRMI.receivedAppendEntries = false;
        serverRMI.setVotedFor("");
    }

    /**
     * This constructor is used when a Follower wants to "remember" which node it has given its vote, so when:
     * 1. A candidate becomes a follower after having received a valid appendEntry
     * 2. A follower maintains this role after having received a valid appendEntry or granted its vote to someone
     * @param votedFor the identifier of the the node which the follower has voted for
     */
    public Follower(ServerRMI serverRMI, String votedFor) {
        super(serverRMI);
        alreadyLeader = false;
        serverRMI.grantedVote = false;
        serverRMI.receivedAppendEntries = false;
        serverRMI.setVotedFor(votedFor);
    }

    @Override
    public void run() {
        //a follower simply sleeps until timeouts (and then switch to candidate or stay follower)
        try {
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                System.out.println(serverRMI.id+" is a follower");
            while(true)
                Thread.sleep(5000);
        } catch (InterruptedException e) {}
    }

    /**
     * If election timeout elapses without receiving appendEntries
     * RPC from current leader or granting vote to some candidate: convert to candidate and starts a new (or join) election
     * @return the new state of the server: a Candidate or a Follower
     */
    @Override
    public ServerState timeout() {
        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
            System.out.println(serverRMI.id+" follower timeout!");
        if(serverRMI.grantedVote||serverRMI.receivedAppendEntries) {
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                System.out.println(serverRMI.id+" stays follower since grantedVote="+serverRMI.grantedVote+" and receivedAppendEntries="+serverRMI.receivedAppendEntries);
            return new Follower(serverRMI, serverRMI.getVotedFor());
        }
        else {
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
                System.out.println(serverRMI.id+" becomes a candidate!");
            return new Candidate(serverRMI);
        }
    }

    @Override
    public ServerState taskCompleted() throws UnexpectedStateException{
        throw new UnexpectedStateException();
    }

    @Override
    public long generateTimeout() {
        return timeoutGenerator.nextInt(serverRMI.randomElectionTimeout)+serverRMI.lowerBoundElectionTimeout;
    }
}
