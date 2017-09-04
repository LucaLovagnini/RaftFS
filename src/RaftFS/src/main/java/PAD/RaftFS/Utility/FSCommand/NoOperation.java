package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;

import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by luca on 11/03/15.
 */

/**
 * This operation does NOTHING. It is used by a Leader as soon as possible in order to commit and entry for the
 * current term and satisfy the first read-only condition. It's a write operation.
 */
public class NoOperation extends FSCommand {
    public NoOperation() throws PathFormatException {
        super(true, null, null);
    }

    @Override
    public void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI) {
        //do nothing
    }

    @Override
    public void callAnswer() throws RemoteException {
        //do nothing
    }

}
