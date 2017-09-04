package PAD.RaftFS.Client;

import PAD.RaftFS.Utility.ClusterElement;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Created by luca on 02/04/15.
 */
public class UploadTask implements Callable<ClusterElement> {

    private File localFile;
    private String fileName;
    public ClusterElement clusterElement;

    public UploadTask(File localFile, String fileName, ClusterElement clusterElement) {
        this.localFile = localFile;
        this.fileName = fileName;//already converted from / to |
        this.clusterElement = clusterElement;
    }

    @Override
    public ClusterElement call() throws Exception {
        ClusterElement result;//null = uploading failed, server where the file was uploaded otherwise
        try (FileInputStream file = new FileInputStream(localFile)) {
            try {
                System.out.println("Connecting to "+clusterElement.getAddress()+"/"+clusterElement.getPort());
                FTPClient ftpClient = SingleClientRequest.createClient(clusterElement.getAddress(),clusterElement.getPort());
                ftpClient.enterLocalPassiveMode();
                System.out.println("Task uploading file on "+clusterElement.getId());
                if(ftpClient.storeFile(fileName+".tmp", file)&&//so the Garbage Collector will not delete it during upload
                    ftpClient.rename(fileName+".tmp", fileName)//if the upload is successful, rename it with the original name
                )
                    result = clusterElement;
                else {
                    System.out.println("File " + fileName + " not uploaded on" + clusterElement.getId());
                    result = null;
                }
            } catch (IOException e) {
                System.err.println("Error uploading "+localFile+" on "+clusterElement.getId());
                result = null;
            }
        } catch (IOException e) {
            System.err.println("Error with local file " + localFile.getName());
            result = null;
        }
        return result;
    }

}
