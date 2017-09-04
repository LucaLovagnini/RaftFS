package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Client.ClientInterface;
import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;
import com.google.common.collect.ImmutableList;

import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by luca on 08/03/15.
 */
public class Ls extends FSCommand{

    //used an immutable object for security reason (if the system will be extended)
    ImmutableList<String> result;

    public Ls(ClientInterface stub, String commandPath) throws PathFormatException {
        super(false,stub,commandPath);
    }

    @Override
    public void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI) {
        //return the list of elements (files and directories) contained in the commandPath directory
        //it returns null if commandPath isn't legal
        result = stateMachineApplier.getElementInDir(commandPath);
        if(result!=null)
            checkResult(true, stateMachineApplier, serverRMI, null);
        else
            checkResult(false, stateMachineApplier, serverRMI, null);
    }

    @Override
    public void callAnswer() throws RemoteException {
        stub.lsResult(result);
    }

}
