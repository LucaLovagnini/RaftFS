package PAD.RaftFS.Server;

import PAD.RaftFS.Utility.FSCommand.FSCommand;
import PAD.RaftFS.Utility.LogEntry;
import PAD.RaftFS.Utility.MethodAnswer;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface ServerInterface extends Remote {
    /**
     * This is the main method during the leader election in Raft. It's invoked by a candidate which wants the vote
     * from a node. Each node can vote for a single node on each term, and cannot vote a candidate which log isn't up to date.
     * A candidate's log is up to date if the term of its last entry is greater than the term of the last entry of the voter's log
     * OR if the terms of the last entries are equals, then the longer one is the more updated
     * @param term the candidate's term
     * @param candidateID the candidate's identifier (hostname)
     * @param lastLogIndex the index of the last candidate's log entry (used if lastLogTerm is equal to voter's lastLogTerm)
     * @param lastLogTerm the term of the last candidate's log entry
     * @throws RemoteException
     */
    public MethodAnswer requestVote(int term, String candidateID, int lastLogIndex, int lastLogTerm) throws RemoteException;

    /**
     * This is the main method used after a leader was elected. It's invoked by the leader and it's used to sends heartbeat
     * to each cluster node. An heartbeat has two purposes:
     * 1. to notify a node of the leader existence (and so avoiding that it becomes a candidate)
     * 2. to append new entries received from clients
     * A leader appends the entries of its log from prevLogIndex to its log size. If the term value of the entry
     * with index equals to prevLogIndex in the follower's log doesn't match with prevLogTerm, then this appendEntries
     * call fails and the leader will proceed with the back-rolling mechanism. This method fails if the prevLogIndex is
     * bigger than the follower's log size. In order words, the leader search the latest entry which matches with the follower's
     * log. isReadOnly is used to satisfy the second read-only condition (look serverRMI.clientRequest for more informations)
     * @param term the leader's term
     * @param leaderID the leader's name (hostname)
     * @param leaderAddress the leader's IP address, used in order to redirect clients to it
     * @param prevLogIndex index of log entry immediately preceding new ones
     * @param prevLogTerm term of prevLogIndex entry
     * @param entries log entries to store
     * @param leaderCommit leader's commit index
     * @param isReadOnly true only if this appendEntries is called as heart beat for the second read-only condition
     * @throws RemoteException
     */
    public MethodAnswer appendEntries(int term, String leaderID, String leaderAddress, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit, boolean isReadOnly) throws RemoteException;

    /**
     * This method is called by a client which wants to submit a File System command. A command can updates the FS or
     * not (and so it's a read-only command). Depending on this factor the command is managed differently: a read-only
     * command isn't write on the log (and so it's not replicated) but it must to satisfy two conditions in order
     * to guarantee linearizability:
     * 1. the leader has to commit at least one entry from its own term (a no-op entry is sufficient). This is necessary
     * since the read operation can be performed after that the State Machine Applier will apply AT LEAST the committed
     * entry from this term (it will works as lower bound)
     * 2. the leader has to be sure that it's still a valid leader in order to guarantee that the returned results are
     * not stale. In order to do so, it sends an heartBeat to each cluster node: if it receives the majority of the answers
     * it's still a leader
     * @param command the FS command submitted by the client
     * @throws RemoteException
     */
    public void clientRequest(FSCommand command) throws RemoteException;
}