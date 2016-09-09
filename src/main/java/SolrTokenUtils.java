import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SolrTokenUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SolrTokenUtils.class);
  private static final String TOKEN_KIND = "solr-dt";

  private static class SolrTokenIdentifier extends AbstractDelegationTokenIdentifier {
    public Text getKind() {
      return new Text(TOKEN_KIND);
    }
  }

  // From HBMRIndexer
  public static Token<SolrTokenIdentifier> getCredentialsToken(String token,
                                                                String serviceName) throws IOException {
    // only need to reconstruct the identifier in order to use it, so just make the password empty.
    return new Token<>(
      token.getBytes(StandardCharsets.UTF_8), new byte[0], new Text(TOKEN_KIND),
      new Text(serviceName));
  }

  // From HBMRIndexer
  public static String getCredentialsString(Credentials creds, String serviceName) throws IOException {
    Token<? extends TokenIdentifier> token = creds.getToken(new Text(serviceName));
    if (token == null) {
      throw new IOException("Unable to locate credentials");
    }
    return new String(token.getIdentifier(), StandardCharsets.UTF_8);
  }

  // From HBMRIndexer
  public static void addDelegationToken(Job job, String zkHost) throws IOException, SolrServerException {
    LOG.info("Making local connection to Solr");
    CloudSolrServer server = new CloudSolrServer(zkHost);

    LOG.info("Initializing job credentials");
    DelegationTokenRequest.Get getToken = new DelegationTokenRequest.Get();
    DelegationTokenResponse.Get getTokenResponse = getToken.process(server);
    String token = getTokenResponse.getDelegationToken();
    Token<? extends TokenIdentifier> credentialsToken = getCredentialsToken(token, zkHost);
    job.getCredentials().addToken(credentialsToken.getService(), credentialsToken);
    job.getConfiguration().setBooleanIfUnset("solr.secure", true);
  }

}
