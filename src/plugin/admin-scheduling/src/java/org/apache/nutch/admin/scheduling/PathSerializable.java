package org.apache.nutch.admin.scheduling;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.fs.Path;


/**
 * A very thin class to declare the {@link Path} Serializable, thus can be used 
 * in scheduling.
 * @author enis.soztutar@agmlab.com
 */
public class PathSerializable extends Path implements Serializable {

	/**
	 * Construct from path
	 * @param path
	 */
	public PathSerializable(Path path) {
		super(path.getParent(), path.getName());
	}
	
	public PathSerializable(Path parent, Path child) {
		super(parent, child);	
	}

	
	public PathSerializable(Path parent, String child) {
		super(parent, child);
		
	}

	public PathSerializable(String parent, Path child) {
		super(parent, child);
		
	}

	public PathSerializable(String parent, String child) {
		super(parent, child);
		
	}

	public PathSerializable(String pathString) {
		super(pathString);	
	}

	/**
	 * Computed automatically 
	 */
	private static final long serialVersionUID = 2618612299235704118L;

	
	
	private synchronized void writeObject(java.io.ObjectOutputStream s)
	throws IOException
	{
		s.defaultWriteObject();
	}

	private synchronized void readObject(java.io.ObjectInputStream s)
	throws IOException, ClassNotFoundException
	{
		s.defaultReadObject();
	}

}
