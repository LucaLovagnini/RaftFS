package PAD.RaftFS.Server;

import PAD.RaftFS.Utility.ClusterElement;
import PAD.RaftFS.Utility.FSCommand.FSCommand;
import com.google.common.collect.ImmutableList;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by luca on 04/03/15.
 */
public class StateMachineApplier extends Thread {

    //since all these fields are REALLY important and dangerous to manage (and many of them are persistent too)
    //they have been all declared as private and no direct get was defined (unless it was inevitable)

    //persistent fields
    //the map below allow to the server to know where is file is stored on the cluster
    //each entry is a pair (fileName,(fileSize,serversList) where serversList contains the list of servers where the file is stored.
    //since each fileName is expressed as an absolute path, it's guaranteed that there cannot be any conflict between keys
    //by default each file is replicated on 2 servers
    private final ConcurrentHashMap<String, Map.Entry<Long, List<ClusterElement>>> fileLocation;
    //this field implements the File System structure: it stores each File System directory and all its files/directories contained
    //each entry is a pair (directoryName,elementsList) where elementsList is the list of files/directories names contained in directoryName
    //notice that the elements contained in elementsList aren't absolute path, but simple names
    //since each directoryName is expressed as an absolute path, it's guaranteed that there cannot be any conflict between keys
    private final ConcurrentHashMap<String, ArrayList<String>> fsStructure;
    //every time that commitIndex is updated, then all the commit entries are passed to MachineApplier, which apply them
    //since it's a blocking queue, the MachineApplier's task is suspended it there aren't operation to execute
    //it's resumed as soon as a write operation is committed (and added to this queue)
    private final LinkedBlockingQueue<Map.Entry<FSCommand,Integer>> scheduledOperations;

    //volatile fields
    private final ServerRMI serverRMI;
    private final Lock schedulerLock = new ReentrantLock();//this lock is needed since the addAll() operation is thread safe but it's not guaranteed to be atomic
    final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();//used in order to guarantee consistency between concurrent read and write operations

    /**
     * Constructor used  when this is machine is executed for the first time (see ServerRMI.setPersistentFields())
     */
    public StateMachineApplier(ServerRMI serverRMI){
        this.serverRMI = serverRMI;
        fileLocation = new ConcurrentHashMap<>();
        updateFileLocation();
        fsStructure = new ConcurrentHashMap<>();
        fsStructure.put("/",new ArrayList<String>());
        updateFsStructure();
        scheduledOperations = new LinkedBlockingQueue<>();
        updateScheduledOperations();
    }

    /**
     * Constructor used when this machine isn't the first time that it is used (see ServerRMI.setPersistentFields())
     */
    public StateMachineApplier(ServerRMI serverRMI,
            ConcurrentHashMap<String, Map.Entry<Long, List<ClusterElement>>> fileLocation,
            ConcurrentHashMap<String, ArrayList<String>> fsStructure,
            LinkedBlockingQueue<Map.Entry<FSCommand, Integer>> scheduledOperations) {
        this.serverRMI = serverRMI;
        this.fileLocation = fileLocation;
        this.fsStructure = fsStructure;
        this.scheduledOperations = scheduledOperations;
    }

    public void createFile(String fileName, Map.Entry<Long, List<ClusterElement>> replicationList){
        fileLocation.put(fileName,replicationList);
        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
            System.out.println(serverRMI.id+" file "+fileName+" successfully created");
        updateFileLocation();
    }

    public void deleteFile(String commandPath){
        Map.Entry<Long,List<ClusterElement>> fileMetadata = fileLocation.get(commandPath);
        for(ClusterElement server : fileMetadata.getValue()) {//free the space used by the file from each server where it's replicated
            serverRMI.getCluster().get(serverRMI.getCluster().indexOf(server)).freeDiskSpace(fileMetadata.getKey());
            serverRMI.updateCluster();//update the cluster list on file
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                System.out.println(serverRMI.id+" freed "+fileMetadata.getKey()+" on "+server.getId()+", actual space="+server.getDiskSpace());
        }
        fileLocation.remove(commandPath);
        updateFileLocation();
    }

    public void addReplicationList(String fileName , List<ClusterElement> replicationList){
        List<ClusterElement> actualReplicationList = fileLocation.get(fileName).getValue();
        actualReplicationList.addAll(replicationList);
        updateFileLocation();
        if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
            System.out.println(serverRMI.id+" servers successfully added to "+fileName+"'s entry");
    }

    /**
     * Used by Get command. The immutable object grant that there cannot exist any side effect
     * @param commandPath
     * @return
     */
    public AbstractMap.SimpleImmutableEntry<Long, List<ClusterElement>> getEntry(String commandPath) {
        return (AbstractMap.SimpleImmutableEntry<Long, List<ClusterElement>>) fileLocation.get(commandPath);
    }

    public boolean containsFile(String fileName){
        return fileLocation.keySet().contains(fileName);
    }

    public boolean isFileReplicatedOnMachine(String fileName, ClusterElement clusterElement){
        Map.Entry<Long, List<ClusterElement>> fileEntry = fileLocation.get(fileName);
        return fileEntry.getValue().contains(clusterElement);
    }

    public boolean isFullyReplicated(String commandPath) {
        return fileLocation.get(commandPath).getValue().size()>=serverRMI.getReplicationFactor();
    }

    public void createDir(String dirName){
        fsStructure.put(dirName,new ArrayList<String>());
        updateFsStructure();
    }

    public void deleteDir(String dirName){
        fsStructure.remove(dirName);
        updateFsStructure();
    }

    public void putDirElement(String dirName, String elementName){
        List<String> dirContent = fsStructure.get(dirName);
        dirContent.add(elementName);
        updateFsStructure();
    }

    public boolean deleteDirElement(String dirName, String elementName){
        List<String> dirContent = fsStructure.get(dirName);
        if(dirContent.remove(elementName)) {
            updateFsStructure();
            return true;
        }
        else
            return false;
    }


    public ImmutableList<String> getElementInDir(String commandPath) {
        if(fsStructure.get(commandPath)==null)
            return null;
        else
            return new ImmutableList.Builder<String>().
                addAll(fsStructure.get(commandPath)).build();
    }

    public boolean containsDir(String dirName){
        return fsStructure.keySet().contains(dirName);
    }

    public boolean dirContainsElement (String dirName , String element){
        return fsStructure.get(dirName).contains(element);
    }

    public boolean isEmptyDir(String commandPath) {
        return fsStructure.get(commandPath).isEmpty();
    }

    private void updateFileLocation() {
        try (ObjectOutputStream fileLocationFile = new ObjectOutputStream(new FileOutputStream(serverRMI.id + "/fileLocation"))) {
            fileLocationFile.writeObject(fileLocation);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateFsStructure() {
        try(ObjectOutputStream fsStructureFile = new ObjectOutputStream(new FileOutputStream(serverRMI.id+"/fsStructure"))){
            fsStructureFile.writeObject(fsStructure);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateScheduledOperations() {
        try(ObjectOutputStream scheduledOperationsFile = new ObjectOutputStream(new FileOutputStream(serverRMI.id+"/scheduledOperations"))){
            scheduledOperationsFile.writeObject(scheduledOperations);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void scheduleCommands(List<Map.Entry<FSCommand, Integer>> commandList)
    {
        schedulerLock.lock();
        scheduledOperations.addAll(commandList);
        updateScheduledOperations();
        schedulerLock.unlock();
    }

    private void executeWriteCommand(FSCommand command, int logIndex) {
        readWriteLock.writeLock().lock();
        command.executeCommand(this, serverRMI);
        if(command.isWriteCommand())//read command doesn't update lastApplied (since they're not written in the log)
            serverRMI.setLastApplied(logIndex);
        readWriteLock.writeLock().unlock();
    }

    public void executeReadCommand (FSCommand command)
    {
        readWriteLock.readLock().lock();
        command.executeCommand(this,serverRMI);
        readWriteLock.readLock().unlock();
    }


    /**
     * This method is called as soon the server is up. It get and execute each write command committed by the Raft algorithm
     * and updates lastApplied
     */
    public synchronized void run(){
        while (true) {
            Map.Entry<FSCommand,Integer> pair;
            FSCommand command;
            int logIndex;
            try {
                pair = scheduledOperations.take();//blocking operation
                command = pair.getKey();
                logIndex = pair.getValue();//for reads operations it's -1 (since they're not written in the log)
                if(logIndex>serverRMI.getLastApplied())
                    executeWriteCommand(command,logIndex);
            } catch (InterruptedException e) {
                System.out.println("StateMachineApplier"+serverRMI.id+" interrupted while waiting new entries");
                break;
            }
        }
    }

    public List<ClusterElement> getServers(String commandPath) {
        return fileLocation.get(commandPath).getValue();
    }
}
