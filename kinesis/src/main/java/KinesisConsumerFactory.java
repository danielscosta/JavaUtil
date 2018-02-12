
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

import model.FilterConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * @author daniel.costa, geovane.ferreira
 *
 */
public class KinesisConsumerFactory {

    private static final Logger _logger = LoggerFactory.getLogger(KinesisConsumerFactory.class);

    private static final String VERSION = "1.0.0";

    public KinesisConsumerFactory(String nameapplication, String streamname, String regionname, AWSCredentialsProvider credentialsProvider, Class<? extends Runnable> listener, InitialPositionInStream initialPositionInStream, FilterConsumer filter){

        new Thread(new Worker(new IRecordProcessorFactory() {
                                    @Override
                                    public IRecordProcessor createProcessor() {
                                        return new KinesisConsumer(listener, filter);
                                    }
                                },  new KinesisClientLibConfiguration(nameapplication,
                                    streamname,
                                    credentialsProvider,
                                    String.valueOf(UUID.randomUUID()))
                                        .withRegionName(regionname)
                                        .withCommonClientConfig(getClientConfigWithUserAgent(nameapplication))
                                        .withInitialPositionInStream(initialPositionInStream)
                                        .withMetricsLevel(MetricsLevel.DETAILED))).start();

        _logger.info("Starting Consumer for " + streamname + " on " + regionname);
    }

    public KinesisConsumerFactory(String nameapplication, String streamname, String regionname, AWSCredentialsProvider credentialsProvider, Class<? extends Runnable> listener, InitialPositionInStream initialPositionInStream) {
        this(nameapplication, streamname, regionname, credentialsProvider, listener, initialPositionInStream, null);
    }

    private ClientConfiguration getClientConfigWithUserAgent(String APPLICATION_NAME) {
        if(APPLICATION_NAME.isEmpty())return null;
        final ClientConfiguration config = new ClientConfiguration();
        final StringBuilder userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT);

        // Separate fields of the user agent with a space
        userAgent.append(" ");
        // Append the application name followed by version number of the sample
        userAgent.append(APPLICATION_NAME);
        userAgent.append("/");
        userAgent.append(VERSION);

        config.setUserAgentPrefix(userAgent.toString());
        config.setUserAgentSuffix(null);

        return config;
    }

}
