package com.sicnu.frank.mapReduce.beanTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class flowReducer extends Reducer<Text,flowBean,Text,flowBean> {
     flowBean v =new flowBean();
    @Override
    protected void reduce(Text key, Iterable<flowBean> values, Context context) throws IOException, InterruptedException {
        //累加求和
        long sum_upFlow = 0;
        long sum_downFlow= 0;

        for (flowBean flowBean:values)
        {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }

        v.setUpFlow(sum_upFlow);
        v.setDownFlow(sum_downFlow);
        v.setSumFlow(sum_upFlow+sum_downFlow);
        context.write(key,v);
    }
}
