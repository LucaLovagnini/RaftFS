package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Client.ClientInterface;
import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;
import PAD.RaftFS.Utility.RequestFailed;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by luca on 08/03/15.
 */

//TODO: give an unique identifier to each client request in order to avoid that it's executed more than once

public abstract class FSCommand implements Serializable{
    protected boolean writeCommand;//a command can be a write one or a read one
    protected ClientInterface stub;//reference to the stub of the client which submitted the command. it will be used to answer to the client
    protected String commandPath;//each command has an absolute path as argument

    public FSCommand(boolean writeCommand , ClientInterface stub , String commandPath) throws PathFormatException{
        if(commandPath!=null && !commandPath.matches("(/\\w+)*/\\w+(\\.\\w+)?|/"))
            throw new PathFormatException(commandPath + " isn't an absolute path");
        this.writeCommand = writeCommand;
        this.stub = stub;
        this.commandPath = commandPath;
    }

    /**
     * This method will be called by StateMachineApplier, and execute a FSCommand. Since for writes operations
     * are replicated between the nodes, only the Leader will reply to the Client which submitted the request,
     * and for this reason a reference to the relative serverRMI is needed (it contains leaderID and node's id).
     * Notice that if the command is a read-only operation and the node becomes a Follower while executing the
     * query (before it was a leader for sure thanks to the two reads-only conditions), then the
     * ClientInterface.requestFailed() callback is invoked (since the data could be stale).
     * @param stateMachineApplier
     * @param serverRMI
     */
    public abstract void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI);

    public boolean isWriteCommand() {
        return writeCommand;
    }

    public ClientInterface getStub() {
        return stub;
    }

    /**
     * This method is called as last instruction by each FS command. It checks that commandPath is a feasible argument
     * (which already or doesn't exist). In addiction, if this node is the leader, answer to the client (invoking callAnswer() )
     * @param success set to true if the command was successful, false if it failed because commandPath isn't feasible
     * @param stateMachineApplier used to establish the reason because commandPath isn't feasible
     * @param serverRMI used to understand if this node is the leader
     * @param errorMessage it defines a non standard error (for instance a file is already full replicated). it is set to null otherwise.
     */
    public void  checkResult(boolean success, StateMachineApplier stateMachineApplier , ServerRMI serverRMI , String errorMessage) {
        try {
            String message = "";
            if(errorMessage != null) {//operation failed for a non-standard reason
                message = errorMessage;
                success = false;//success should have been already set to false, but set it anyway
            }
            else if(!success)//operation failed for a standard reason
            {
                if(stateMachineApplier.containsFile(commandPath))
                    message = "command failed: "+commandPath+" is a file";
                else if(stateMachineApplier.containsDir(commandPath))
                    message = "command failed: "+commandPath+" is a directory";
                else
                    message = "command failed: "+commandPath+" doesn't exists";
            }
            //if this node is the leader, then answer to the client
            if(serverRMI.id.equals( serverRMI.getLeaderId() )) {
                if (!serverRMI.heartBeatRound())
                    stub.requestFailed(new RequestFailed(serverRMI.id + " isn't Leader anymore!"));
                else if (!success)
                    stub.requestFailed(new RequestFailed(message));
                else
                    callAnswer();
            }

        } catch (RemoteException e) {
        }
    }
    public abstract void callAnswer() throws RemoteException;
}
