package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Client.ClientInterface;
import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;
import PAD.RaftFS.Utility.ClusterElement;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by luca on 08/03/15.
 */
public class Remove extends FSCommand {

    boolean isFile = false;//true if the element removed is a directory
    List<ClusterElement> serversFile;//this list is used only to remove a file

    public Remove(ClientInterface stub, String commandPath) throws PathFormatException {
        super(true,stub,commandPath);
    }

    @Override
    public void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI) {
        //if commandPath isn't a file and commandPath isn't a directory or is not empty, then command failed
        if(!stateMachineApplier.containsFile(commandPath) && !stateMachineApplier.containsDir(commandPath))
            checkResult(false,stateMachineApplier,serverRMI,null);
        else if(stateMachineApplier.containsDir(commandPath) && !stateMachineApplier.isEmptyDir(commandPath))
            checkResult(false,stateMachineApplier,serverRMI,"Error removing "+commandPath+": the directory isn't empty");
        else {
            //remove the element from the list of its parent directory
            String fileName = commandPath.substring(commandPath.lastIndexOf("/")+1 , commandPath.length());
            String parentDirectoryPath = commandPath.lastIndexOf("/")==0 ? "/" : commandPath.substring(0,commandPath.lastIndexOf("/"));
            if(!stateMachineApplier.containsDir(parentDirectoryPath))
                checkResult(false,stateMachineApplier,serverRMI,"Error removing "+commandPath+": the element exists but the parent directory doesn't");
            else if(!stateMachineApplier.deleteDirElement(parentDirectoryPath,fileName))
                checkResult(false,stateMachineApplier,serverRMI,"Error removing "+commandPath+": the element doesn't appear in the parent directory");
            else {
                if(stateMachineApplier.containsFile(commandPath)) {//if the element is a file
                    isFile = true;
                    serverRMI.garbageCollector.blackList.remove(commandPath);//look GarbageCollector.blackList description
                    serversFile = stateMachineApplier.getServers(commandPath);
                    stateMachineApplier.deleteFile(commandPath);//delete fileLocation entry with commandPath as key
                }
                else//otherwise commandPath is a directory
                    stateMachineApplier.deleteDir(commandPath);//delete fsStructure entry with commandPath as key
                checkResult(true, stateMachineApplier, serverRMI, null);
            }
        }
    }

    @Override
    public void callAnswer() throws RemoteException {
        if(!isFile)
            stub.messageResult("directory "+commandPath+" successfully removed");
        else
            stub.removeFileResult(serversFile);
    }

}
