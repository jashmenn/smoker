package smoker.udf;

import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public abstract class ClojureUDTF extends GenericUDTF {
  /**
   * Passes an output row to the collector
   *
   * We've got to do this in java because in our superclass it is
   * defined as both protected and final. This means we can't use
   * gen-class :exposes-methods (won't work for final). We can't even
   * use :exposes and access the collector directly because collector
   * is private in GenericUDTF.
   * 
   * @param o
   * @throws HiveException
   */
  public void emit(Object o) throws HiveException {
    super.forward(o);
  }

}
