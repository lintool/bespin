package io.bespin.java.mapreduce.bfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for parallel breadth-first search.
 *
 * @author Jimmy Lin
 */
public class BfsNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // Complete structure.
    Distance((byte) 1),  // Distance only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

  private static final Type[] mapping = new Type[] { Type.Complete, Type.Distance, Type.Structure };

  private Type type;
  private int nodeid;
  private int distance;
  private ArrayListOfIntsWritable adjacencyList;

  public BfsNode() {}

  public int getDistance() {
    return distance;
  }

  public void setDistance(int d) {
    distance = d;
  }

  public int getNodeId() {
    return nodeid;
  }

  public void setNodeId(int n) {
    nodeid = n;
  }

  public ArrayListOfIntsWritable getAdjacencyList() {
    return adjacencyList;
  }

  public void setAdjacencyList(ArrayListOfIntsWritable l) {
    adjacencyList = l;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    type = mapping[in.readByte()];

    nodeid = in.readInt();

    if (type.equals(Type.Distance)) {
      distance = in.readInt();
      return;
    }

    if (type.equals(Type.Complete)) {
      distance = in.readInt();
    }

    adjacencyList = new ArrayListOfIntsWritable();
    adjacencyList.readFields(in);
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.val);
    out.writeInt(nodeid);

    if (type.equals(Type.Distance)) {
      out.writeInt(distance);
      return;
    }

    if (type.equals(Type.Complete)) {
      out.writeInt(distance);
    }

    adjacencyList.write(out);
  }

  @Override
  public String toString() {
    return String.format("{%d %d %s}", nodeid, distance, (adjacencyList == null ? "[]"
        : adjacencyList.toString(10)));
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static BfsNode create(DataInput in) throws IOException {
    BfsNode m = new BfsNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static BfsNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
