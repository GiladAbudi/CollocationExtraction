package Tools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComparableKey implements WritableComparable<ComparableKey> {
    private Text w1;
    private Text w2;
    private Text decade;
    private Text likelihood;

    public ComparableKey(String w1, String w2,String decade,String likelihood){
        this.w1=new Text(w1);
        this.w2=new Text(w2);
        this.decade=new Text(decade);
        this.likelihood=new Text(likelihood);
    }

    public ComparableKey(){
        this.w1=new Text("");
        this.w2=new Text("");
        this.decade=new Text("");
        this.likelihood=new Text("");
    }

    public ComparableKey(Text w1, Text w2, Text decade, Text likelihood) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = decade;
        this.likelihood = likelihood;
    }

    public Text toText(){
        return new Text(this.toString());
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public Text getDecade() {
        return decade;
    }

    public Text getLikelihood() {
        return likelihood;
    }

    public Double getDoubleLikelihood(){
        return Double.parseDouble(likelihood.toString());
    }

    @Override
    public int compareTo(ComparableKey other) {
        if(decade.compareTo(other.getDecade()) > 0) {
            return -1;
        } else if(decade.compareTo(other.getDecade()) < 0) {
            return 1;
        } else {
            if(getDoubleLikelihood()<other.getDoubleLikelihood()) {
                return 1;
            } else if(getDoubleLikelihood()>other.getDoubleLikelihood()) {
                return -1;
            } else {
                if(w1.compareTo(other.getW1()) < 0) {
                    return -1;
                } else if(w1.compareTo(other.getW1()) > 0) {
                    return 1;
                } else {
                    if(w2.compareTo(other.getW2()) > 0) {
                        return -1;
                    } else if(w2.compareTo(other.getW2()) < 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }
        }
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        w1.write(dataOutput);
        w2.write(dataOutput);
        decade.write(dataOutput);
        likelihood.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        decade.readFields(dataInput);
        likelihood.readFields(dataInput);
    }

    @Override
    public String toString() {
        return w1.toString() + " " +w2.toString() + " " + decade.toString() + " " + likelihood.toString()+ " ";
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ComparableKey)) return false;
        ComparableKey other = (ComparableKey) o;
        return w1.equals(other.getW1())
                && w2.equals(other.getW2())
                && decade.equals(other.getDecade())
                && likelihood.equals(other.getLikelihood());
    }
}
