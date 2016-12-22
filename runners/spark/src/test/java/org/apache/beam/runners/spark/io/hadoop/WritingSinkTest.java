// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.apache.beam.runners.spark.io.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.hdfs.HDFSFileSink;
import org.apache.beam.sdk.io.hdfs.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

public class WritingSinkTest {

    /** A "log" of exceptions for debugging traces. */
    public static ArrayList<Exception> logsAndTraces = new ArrayList<>();

    /** Log the message and the stack trace getting there. */
    public static synchronized void log(int indent, String msg) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < indent; i++)
            sb.append("     ");
        sb.append(msg);
        // save it for later
        logsAndTraces.add(new Exception(sb.toString()));
        // but also print it out now
        System.out.println(sb.toString());
        System.out.flush();
    }

    /** Dumps info from logsAndTraces to the console. */
    public static void printLogs() {
        System.out.println("=== SUMMARY ===");
        for (Exception e : logsAndTraces)
            System.out.println(e.getMessage());
        // Print out the traces for the Create Writer
        for (Exception e : logsAndTraces)
            if (e.getMessage().contains("Create Writer")) {
                System.out.println("============================================");
                e.printStackTrace(System.out);
            }
    }

    @Before
    public void setup() {
        logsAndTraces.clear();
    }

    @Test
    public void testCustomSink_Direct() throws IOException {
        final Pipeline p = TestPipeline.create();

        PCollection<String> input = p.apply( //
                Create.of("one", "two"));
        input.apply(Write.to(new MySink("sink")));

        // And run the test.
        p.run().waitUntilFinish();

        printLogs();
    }

    @Test
    public void testCustomSink_Spark() throws IOException {

        SparkConf conf = new SparkConf();
        conf.setAppName("testBeamWrite");
        conf.setMaster("local[1]");
        conf.set("spark.hadoop." + CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "file:///tmp/localfs");
        SparkContextOptions options = PipelineOptionsFactory.as(SparkContextOptions.class);
        options.setRunner(SparkRunner.class);
        options.setUsesProvidedSparkContext(true);
        options.setProvidedSparkContext(new JavaSparkContext(conf));
        final Pipeline p = Pipeline.create(options);

        PCollection<String> input = p.apply( //
                Create.of("one", "two"));
        input.apply(Write.to(new MySink("sink")));

        // And run the test.
        p.run().waitUntilFinish();

        // Check the expected values.
        printLogs();

    }

    @Test
    public void testHdfsSink_Spark() throws IOException {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        String fileSpec = fs.getUri().resolve("/tmp/test/input.csv").toString();

        fs.delete(new Path(fileSpec), true);

        SparkConf conf = new SparkConf();
        conf.setAppName("testBeamWrite");
        conf.setMaster("local");
        conf.set("spark.hadoop." + CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "file:///tmp/localfs");
        SparkContextOptions options = PipelineOptionsFactory.as(SparkContextOptions.class);
        options.setRunner(SparkRunner.class);
        options.setUsesProvidedSparkContext(true);
        options.setProvidedSparkContext(new JavaSparkContext(conf));
        final Pipeline p = Pipeline.create(options);

        PCollection<KV<NullWritable, Text>> input = p.apply( //
                Create.of(//
                        KV.<NullWritable, Text> of(NullWritable.get(), new Text("one")), //
                        KV.<NullWritable, Text> of(NullWritable.get(), new Text("two"))) //
                        .withCoder(KvCoder.of(WritableCoder.of(NullWritable.class), //
                                WritableCoder.of(Text.class))));
        input.apply(Write.to(new HDFSFileSink(fileSpec, TextOutputFormat.class, new Configuration())));

        // And run the test.
        p.run().waitUntilFinish();
    }

    public static class MySink extends Sink<String> {

        public static AtomicInteger count = new AtomicInteger();

        public final String name;

        MySink(String name) {
            this.name = name;
        }

        @Override
        public void validate(PipelineOptions pipelineOptions) {

        }

        @Override
        public MyWriteOperation createWriteOperation(PipelineOptions pipelineOptions) {
            MyWriteOperation w = new MyWriteOperation();
            log(0, "Create WriteOp (" + w.name + ")");
            return w;
        }

        public class MyWriteOperation extends Sink.WriteOperation<String, Integer> {

            public final int uid;

            public final String name;

            public MyWriteOperation() {
                uid = count.incrementAndGet();
                name = MySink.this.name + "/" + "WriteOp" + uid;
            }

            @Override
            public void initialize(PipelineOptions pipelineOptions) throws Exception {
                log(1, "Initialize WriteOperation (" + name + ") ");
            }

            @Override
            public void finalize(Iterable<Integer> iterable, PipelineOptions pipelineOptions) throws Exception {
                log(1, "Finalize WriteOperation (" + name + ") " + iterable);
            }

            @Override
            public MyWriter createWriter(PipelineOptions pipelineOptions) throws Exception {
                MyWriter w = new MyWriter();
                log(1, "Create Writer (" + w.name + ")");
                return w;
            }

            @Override
            public Sink<String> getSink() {
                return MySink.this;
            }

            @Override
            public Coder<Integer> getWriterResultCoder() {
                return BigEndianIntegerCoder.of();
            }

            public class MyWriter extends Sink.Writer<String, Integer> {

                public final int uid;

                public final String name;

                private int writes = 0;

                public MyWriter() {
                    uid = count.incrementAndGet();
                    name = MyWriteOperation.this.name + "/" + "Writer" + uid;
                }

                @Override
                public void open(String s) throws Exception {
                    log(2, "Open (" + name + "): " + s);
                }

                @Override
                public void write(String s) throws Exception {
                    writes++;
                    log(2, "Write (" + name + "): " + s);
                }

                @Override
                public Integer close() throws Exception {
                    log(2, "Close (" + name + ")");
                    return uid;// writes;
                }

                @Override
                public MyWriteOperation getWriteOperation() {
                    return MyWriteOperation.this;
                }
            }
        }
    }
}
