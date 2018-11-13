package pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PageText implements WritableComparable<PageText> {

    public Text page; // page Text
    public FloatWritable rank;// get PR
    private IntWritable count;// outlink length

    public Text getPage() {
        return page;
    }

    public FloatWritable getRank() {
        return rank;
    }

    public IntWritable getCount() {
        return count;
    }

    public PageText() {
        this(new Text(), new FloatWritable(), new IntWritable());
    }

    public PageText(String page, float rank, int count) {
        this(new Text(page), new FloatWritable(rank), new IntWritable(count));
    }

    public PageText(Text page, FloatWritable rank, IntWritable count) {
        super();
        this.page = page;
        this.rank = rank;
        this.count = count;
    }

    public PageText(Text page) {
        super();
        this.page = page;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        page.readFields(in);
        rank.readFields(in);
        count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        page.write(out);
        rank.write(out);
        count.write(out);
    }

    @Override
    public int compareTo(PageText arg0) {
        // TODO Auto-generated method stub
        PageText other = (PageText) arg0;
        if (this.getRank().get() > other.getRank().get())
            return -1;
        if (this.getRank().get() < other.getRank().get())
            return 1;
        return 0;
    }

    @Override
    public String toString() {
//        return super.toString();
//        return this.getRank().get()+";"+this.getCount().get();
        return this.getPage()+","+this.getRank();
    }
}

