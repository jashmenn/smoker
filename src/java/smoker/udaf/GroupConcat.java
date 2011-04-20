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

public class GroupConcat extends UDAF{

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
        public boolean iterate(Text o,  IntWritable N) {
                if (o!=null) {
                        if(mEmpty) {
                                mOutput = new Text(N+" "+o.toString());
                                mEmpty = false;
                        } else { String temp = mOutput.toString() + "\t" + N + " " + o.toString();
                                String[] split = temp.split("\t");
                                Arrays.sort(split);
                                String sorted = split[0];
                                for (int i = 1; i < split.length; i++)
                                {
                                        sorted = sorted + "\t" + split[i];
                                }
                                mOutput.set(sorted);
                        }
                }
                return true;
        }
        public Text terminatePartial() {return mEmpty ? null : mOutput;}
        public boolean merge(Text o) {
                if (o!=null) {
                        if(mEmpty) {
                            mOutput = new Text(o.toString());
                            mEmpty = false;
                        } else { String temp = mOutput.toString() + "\t" + o.toString();
                                String[] split = temp.split("\t");
                                Arrays.sort(split);
                                String sorted = split[0];
                                for (int i = 1; i < split.length; i++)
                                {
                                        sorted = sorted + "\t" + split[i];
                                }
                                mOutput.set(sorted);
                        }
                }
                return true;
        }
        public Text terminate() {return mEmpty ? null : mOutput;}
}
}
