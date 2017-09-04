package PAD.RaftFS.Server;


import java.util.concurrent.*;

/**
 * Created by luca on 17/02/15.
 */
public class Executor extends Thread{
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future;
    ServerState state;
    ServerRMI serverRMI;
    boolean isDown;

    /**
     * Since there is a cyclic dependency between the Executor and the ServerRMI, this method sets the serverRMI relative
     * to this executor and in addiction set the server's initial state as follower
     * @param serverRMI
     */
    public void setInitialState(ServerRMI  serverRMI)
    {
        this.serverRMI = serverRMI;
        state = new Follower(serverRMI);
    }

    /**
     * This method forcefully changes the server's state to a new one: it interrupts the old task, wait that it's
     * actually stopped and then submit the new state
     * @param newState the new server's state
     */
    public synchronized void changeState(ServerState newState)
    {
        future.cancel(true);
        executor.shutdownNow();
        try {
            if(!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
            {
                System.out.println("HELP!");
                Thread.currentThread().interrupt();
            }
            else
            {
                executor = Executors.newSingleThreadExecutor();
                state = newState;
                future = executor.submit(state);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * This method is the main Executor's method. The main task of an Executor is to execute and switch the server's state.
     * Its task consists in the following cycle, which is repeated forever (unless it's interrupted):
     * 1. submit the server state's task. The task submission returns a future object relative to the submitted task
     * 2. if the task is normally completed, then change the server's state, depending on the state returned by taskCompleted()
     *    this case is possible only for the candidate state, where the node successfully get the majority of the votes and
     *    switch from candidate to leader or if it becomes a follower after having left the election (because it has found a newer term)
     * 3. otherwise, if the timeout elapses (which value is different depending on the server's state), then forcefully interrupt
     *    the server's state task and then switch to the state given by timeout(). These are the possible cases:
     *    a. a follower switches to candidate if it has not received a valid appendEntries or granted its vote to someone,
     *       stay as follower otherwise (resubmit the same task)
     *    b. a candidate switches to follower if it has received a valid appendEntries, start a new election otherwise
     *       (it didn't win the election but no valid leader contacted it)
     *    c. a leader simply resubmit the same task (continue to sends appendEntries to each node)
     */
    public void run()
    {
        future = executor.submit(state);//
        serverRMI.isReady = true;//now this server can receives
        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.crazy)>=0)
            System.out.println(serverRMI.id+" is ready");
        while(!Thread.currentThread().isInterrupted()) {
            try {
                future.get(state.generateTimeout(), TimeUnit.MILLISECONDS);//each server state has a different timeout
                changeState(state.taskCompleted());
            }
            catch (CancellationException e){
            }
            catch (InterruptedException e) {
                System.out.println(serverRMI.id+" interrupted!");
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            catch (TimeoutException e) {
                changeState(state.timeout());//state.timeout return the new ServerState when the timeout elapse
            }
            catch (Exception e) {
                System.out.println(serverRMI.id+" exception:");
                e.printStackTrace();
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        isDown = true;
    }
}