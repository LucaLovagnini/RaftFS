package PAD.RaftFS.Server;

import PAD.RaftFS.Utility.ClusterElement;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by luca on 19/03/15.
 */
public class GarbageCollector implements Runnable {
    private final File serverDirectory;
    private final ClusterElement clusterElement;
    private StateMachineApplier stateMachineApplier;
    /*blackList is used for two scenarios:
    1. If a file is successfully upload but the client has not notified the system yet: what it would happen is that the
       file is wrongly deleted, since after a short time the FS is updated.
    2. Since the Garbage Collector cannot delete temp files
    In both cases, if a file would have been deleted by a the garbage collector in two consecutive runs, then the client is down for sure
    (since the Garbage Collector timer is bigger than any client possible command) and so the file can be safely deleted
    Notice that if a file is removed, then it is removed from the blackList too
    */
    public HashSet<String> blackList;
    public GarbageCollector(StateMachineApplier stateMachineApplier , String homeName , String serverName){
        this.stateMachineApplier = stateMachineApplier;
        this.serverDirectory = new File(homeName);
        this.clusterElement = new ClusterElement(serverName);
        this.blackList = new HashSet<>();
    }

    @Override
    public void run() {
        System.out.println(serverDirectory.getName()+" running garbage collector");
        try {
            File[] files = serverDirectory.listFiles();
            for (final File file : files) {
                String fileName = file.getName().replace('@', '/');
                /*there are two possible cases when a file is deleted (or added to the blackList):
                1. the real file exists, but it doesn't in the FS. This case is possible if
                the space of each candidate was entirely occupied during the file upload (so unlucky maaan!)
                or (most probably) a client was gone down after removed the element logically from the FS but not from
                the real one
                2. the real file exists, but it's not stored on this node. This case is possible if THIS node was entirely
                occupied during the file upload (so the file is partially replicated)
                Notice that a file which ends with .tmp is in an upload phase (and so cannot be deleted)
                */
                if((!stateMachineApplier.containsFile(fileName) ||
                        !stateMachineApplier.isFileReplicatedOnMachine(fileName, clusterElement))) {
                    if(blackList.contains(fileName)) {
                        System.out.println("Garbage collector: deleting file " + fileName + " on " + clusterElement.getId());
                        file.delete();
                    }
                    else {
                        System.out.println("Garbage collector: " + fileName + " added to " + clusterElement.getId()+"'s blackList");
                        blackList.add(fileName);
                    }
                }
                else if(blackList.contains(fileName))//file is ok, remove it from the black list
                    blackList.remove(fileName);
            }
        } catch (NullPointerException e) {
            System.err.println("Garbage collector error: " + serverDirectory.getName() + " doesn't exists");
        }
    }
}
