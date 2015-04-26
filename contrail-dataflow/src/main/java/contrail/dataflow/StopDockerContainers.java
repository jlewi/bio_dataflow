package contrail.dataflow;

import java.util.List;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ListContainersParam;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;

/**
 * A simple java program to search for docker containers using
 * certain images stop and remove them.
 *
 * This is intended to help clean up various experiments.
 */
public class StopDockerContainers {
	public static void main(String[] args) throws Exception {
		// Docker must be using a tcp port to work with the spotify client.
	    String dockerAddress = "http://127.0.0.1:4243";
	    DockerClient docker = new DefaultDockerClient(dockerAddress);

	    ListContainersParam param;

	    List<Container> containers = docker.listContainers(ListContainersParam.allContainers());
	    for (Container c : containers) {
	    	if (!c.image().startsWith("google/docker-registry:latest") &&
	    	    !c.image().startsWith("localhost:5010")) {
	    		continue;
	    	}

	        // Stop the registry.
	        // TODO(jlewi): We need to figure out how to make sure this always runs
	        // to avoid leaving the google registry running.
	        try {
	          docker.killContainer(c.id());
	          docker.removeContainer(c.id());
	        } catch (DockerException | InterruptedException e) {
	          // TODO Auto-generated catch block
	          e.printStackTrace();
	        }
	    }
	}
}
