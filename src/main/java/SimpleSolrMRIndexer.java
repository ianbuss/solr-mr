import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SimpleSolrMRIndexer extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleSolrMRIndexer.class);

  public static class SimpleSolrIndexerMapper extends Mapper<LongWritable, Text, Void, Void> {

    private static final int BATCH_SIZE = 100;

    private CloudSolrServer solrServer;
    private List<SolrInputDocument> batch = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      String zkHosts = context.getConfiguration().get("solr.zk");
      String collection = context.getConfiguration().get("solr.collection");
      solrServer = new CloudSolrServer(zkHosts);
      solrServer.setDefaultCollection(collection);

      if (context.getConfiguration().getBoolean("solr.secure", false)) {
        LOG.info("Loading job credentials for clients");
        System.setProperty(
          HttpSolrServer.DELEGATION_TOKEN_PROPERTY,
          SolrTokenUtils.getCredentialsString(context.getCredentials(), zkHosts));
      }
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split("\\|");
      SolrInputDocument inputDocument = new SolrInputDocument();
      inputDocument.addField("id", UUID.randomUUID().toString());
      inputDocument.addField("name", fields[0]);
      inputDocument.addField("address", fields[1]);
      inputDocument.addField("comment", fields[2]);

      batch.add(inputDocument);
      if (batch.size() > BATCH_SIZE) {
        try {
          solrServer.add(batch);
          batch.clear();
        } catch (SolrServerException e) {
          LOG.error("Could not add documents to Solr: {}", e.getMessage());
          throw new IOException("Could not add docs to Solr");
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      try {
        if (!batch.isEmpty()) {
          solrServer.add(batch);
        }
        solrServer.commit();
      } catch (SolrServerException e) {
        LOG.error("Could not add documents or commit to Solr: {}", e.getMessage());
        throw new IOException("Could not commit to Solr");
      }
    }
  }

  private void configureJaas(String jaasFile) {
    System.setProperty("java.security.auth.login.config", jaasFile);
  }


  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("%s <input> <zk> <collection> [<jaasfile>]\n", this.getClass().getName());
      return -1;
    }

    // General config
    Job job = Job.getInstance(getConf());
    job.setJobName("Simple MR Solr Indexer");
    job.setJarByClass(SimpleSolrMRIndexer.class);
    job.setNumReduceTasks(0);

    // I/O formats
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.getConfiguration().set("solr.zk", args[1]);
    job.getConfiguration().set("solr.collection", args[2]);

    // I/O paths
    FileInputFormat.addInputPath(job, new Path(args[0]));

    // MR classes and types
    job.setMapperClass(SimpleSolrIndexerMapper.class);

    if (args.length > 3) {
      configureJaas(args[3]);
      SolrTokenUtils.addDelegationToken(job, args[1]);
    }

    // Run it
    return job.waitForCompletion(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SimpleSolrMRIndexer(), args));
  }
}
