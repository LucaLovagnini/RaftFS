package PAD.RaftFS.Server;

import java.util.Random;

/**
 * Created by luca on 17/02/15.
 */

/**
 * There are three possible state for each server: follower, candidate and leader.
 * Each one of them has a different timeout, execute a different task and changes its state differently
 * (by timeout or completed task)
 */
public abstract class ServerState implements Runnable {
    protected final ServerRMI serverRMI;
    protected Random timeoutGenerator = new Random();//used in generateTimeout
    protected boolean alreadyLeader;//used by the leader

    public ServerState(ServerRMI serverRMI) {
        this.serverRMI = serverRMI;
    }

    public abstract void run();
    public abstract ServerState timeout();
    public abstract ServerState taskCompleted() throws UnexpectedStateException;
    public abstract long generateTimeout();
}
