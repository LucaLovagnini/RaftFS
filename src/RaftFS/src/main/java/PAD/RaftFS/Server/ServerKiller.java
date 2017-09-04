package PAD.RaftFS.Server;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by luca on 30/03/15.
 */
public class ServerKiller extends Thread {

    private Executor serverExecutor;
    private ServerRMI serverRMI;

    public ServerKiller(Executor executor, ServerRMI serverRMI){
        this.serverExecutor = executor;
        this.serverRMI = serverRMI;
    }

    public void run(){
        try {
            sleep(1000);
            serverExecutor.interrupt();
            Registry registry = LocateRegistry.getRegistry();
            registry.unbind(serverRMI.id);
            UnicastRemoteObject.unexportObject((ServerInterface) serverRMI, true);
            serverRMI.logger.info(serverRMI.id + " killed");
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                System.out.println("Resuming "+serverRMI.id);
            Executor executor = new Executor();
            executor.setInitialState(serverRMI);
            serverRMI.setExecutor(executor);
            ServerInterface stub = (ServerInterface) UnicastRemoteObject.exportObject(serverRMI, 0);
            registry.rebind(serverRMI.id, stub);
            executor.start();
        }catch (InterruptedException e){
            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.debug)>=0)
                System.out.println("ServerKiller of "+serverRMI+" interrupted!");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}
