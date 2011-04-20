package smoker.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
/**
 * Copied and pasted from http://www.mail-archive.com/hive-user@hadoop.apache.org/msg02854.html
 * There is probably a better way to do it
 **/
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.Arrays;

public class UDAFGroupConcat extends UDAF{

  public static class GroupConcatStringEvaluator implements UDAFEvaluator {
    private Text mOutput;
    private boolean mEmpty;

    public GroupConcatStringEvaluator() {
      super();
      init();
    }

    public void init() {
      mOutput = null;
      mEmpty = true;
    }

    public boolean iterate(Text o) {
      if (o!=null) {
        if(mEmpty) {
          mOutput = new Text(o);
          mEmpty = false;
        } else {
          mOutput.set(mOutput.toString() + "\u002" + o.toString());
        }
      }
      return true;
    }
    public Text terminatePartial() {return mEmpty ? null : mOutput;}
    public boolean merge(Text o) {return iterate(o);}
    public Text terminate() {return mEmpty ? null : mOutput;}
  }
}
