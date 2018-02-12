
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import model.CallbackConsumer;
import model.FilterConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author daniel.costa, geovane.ferreira
 *
 */
public class KinesisConsumer implements IRecordProcessor {

    private static final Logger _logger = LoggerFactory.getLogger(KinesisConsumer.class);    

    private static final long CHECKPOINT_INTERVAL_MILLIS =  30000L; // 30s
    
    private static ExecutorService executor;

    static {

        int pool = Runtime.getRuntime().availableProcessors();
        try {
            pool = Integer.parseInt(System.getenv("KINESIS_THREAD_POOL"));
        } catch(Exception e) { }

        executor = Executors.newFixedThreadPool(pool);
    }

    private static ConcurrentMap<String, Integer> counter = new ConcurrentHashMap<>();

    private String kinesisShardId;

    private long nextCheckpointTimeInMillis;
    
    private Class<? extends Runnable> _listener;

    private FilterConsumer filterConsumer;

    public KinesisConsumer(Class<? extends Runnable> listener, FilterConsumer filterConsumer){
        this._listener = listener;
        this.filterConsumer = filterConsumer;
    }

    @Override
    public void initialize(String shardId) {
        this.kinesisShardId = shardId;
        this.nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        if(!counter.containsKey(shardId)) counter.put(shardId, 0);
        _logger.info("Initializing record processor for shard: " + shardId);
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {

        for (Record record : records) {
            byte[] bytes = null;
            try {
                if(filterConsumer != null) bytes = filterConsumer.filter(record.getData().array());
                if(bytes == null) continue;
                executor.submit(new CallbackConsumer(_listener.getConstructor(Byte[].class).newInstance(bytes)));
            } catch (Exception e) {
                _logger.warn("Async Fila", e);
            }            
        }

        counter.put(this.kinesisShardId, counter.get(this.kinesisShardId) + records.size());
        long now = System.currentTimeMillis();
        if (now > nextCheckpointTimeInMillis) {
            _logger.info(LocalDateTime.now() + "[" + this.kinesisShardId + "] Process " + counter.get(this.kinesisShardId) + ") UPDATE");
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {}
            this.nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            counter.put(this.kinesisShardId, 0);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        _logger.info("Shutting down record processor for shard: " + kinesisShardId);
        if (reason == ShutdownReason.TERMINATE) {
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {}
        }
    }

}
