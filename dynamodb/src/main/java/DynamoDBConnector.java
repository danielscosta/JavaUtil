
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;

/**
 * @author daniel.costa
 *
 */
public class DynamoDBConnector implements Runnable {

	private static final Logger _logger = LoggerFactory.getLogger(DynamoDBConnector.class);

	public static List<String> tablesCreated = new ArrayList<>();

	private volatile Queue<DynamoBatchObjectController> writeBatchs = null;

	private volatile ConcurrentMap<DynamoBatchObjectController, BatchWriteItemResult> resultWriteBatchOperation = null;

	private Thread thread;

	private AmazonDynamoDB dynamoDB;

	private AtomicBoolean on = new AtomicBoolean(false);

	public DynamoDBConnector(String region, AWSCredentialsProvider credentialsProvider) {
		this.dynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.valueOf(region)).withCredentials(credentialsProvider).build();
		this.writeBatchs = new ConcurrentLinkedQueue<>();
		this.resultWriteBatchOperation = new ConcurrentHashMap<>();
		this.thread = new Thread(this);
	}

	private boolean insert(Map<String, AttributeValue> item, String tableName, boolean isBatch) {
		try {
			checkForTable(tableName);
			if(isBatch && batchInsert(item, tableName)) {
				return true;
			}
			PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(item).withReturnValues(ReturnValue.ALL_OLD);
			PutItemResult putItemResult = this.dynamoDB.putItem(putItemRequest);
			
		} catch (Exception e) {
			_logger.error("Error: ", e);
			return false;
		}

		return true;
	}

	private boolean batchInsert(Map<String, AttributeValue> item, String tableName) {

		if(!on.get()) try { this.thread.start(); } catch (Exception e) {}

		PutRequest putRequest = new PutRequest();
		putRequest.setItem(item);

		WriteRequest writeRequest = new WriteRequest();
		writeRequest.setPutRequest(putRequest);

		DynamoBatchObjectController dynamoBatchObjectController = null;
		while(true) {
			dynamoBatchObjectController = this.writeBatchs.poll();
			if(dynamoBatchObjectController == null) dynamoBatchObjectController = new DynamoBatchObjectController();
			dynamoBatchObjectController.addItem(tableName, writeRequest);

			if(dynamoBatchObjectController.getSize() == 25) {
				BatchWriteItemResult batchWriteItemResult = null;
				try {
					batchWriteItemResult = this.dynamoDB.batchWriteItem(dynamoBatchObjectController.getBatchWriteItemRequest());
				} catch (Exception e) {
					if(e != null && e.getMessage() != null && !e.getMessage().contains("Provided list of item keys contains duplicates")) _logger.error("Batch Operation Exception", e);
					batchWriteItemResult =  new BatchWriteItemResult();
					batchWriteItemResult.setUnprocessedItems(new ConcurrentHashMap<>());
					batchWriteItemResult.getUnprocessedItems().putAll(dynamoBatchObjectController.getBatchWriteItemRequest().getRequestItems());
				}

				this.resultWriteBatchOperation.put(dynamoBatchObjectController, batchWriteItemResult);
				synchronized (dynamoBatchObjectController) { dynamoBatchObjectController.notifyAll(); }
				break;
			} else {
				this.writeBatchs.offer(dynamoBatchObjectController);
				break;
			}
		}

		while(!this.resultWriteBatchOperation.containsKey(dynamoBatchObjectController)) {
			try {
				synchronized (dynamoBatchObjectController) { dynamoBatchObjectController.wait(500); }
			} catch (InterruptedException e) {}
		}

		boolean rt = true;
		BatchWriteItemResult batchWriteItemResult = this.resultWriteBatchOperation.get(dynamoBatchObjectController);
		if(batchWriteItemResult.getUnprocessedItems().containsKey(tableName)) {
			for(WriteRequest writeRequestTemp: batchWriteItemResult.getUnprocessedItems().get(tableName)) {
				if(writeRequestTemp.getPutRequest().getItem().equals(item)) {
					rt = false;
					break;
				}
			}
		}

		dynamoBatchObjectController.process();
		if(dynamoBatchObjectController.isEnd()) {
			this.resultWriteBatchOperation.remove(dynamoBatchObjectController);
		}

		return rt;
	}

	@Override
	public void run() {

		this.on.set(true);
		while(true) {

			int size = this.writeBatchs.size();

			for (int i = 0; i < size; i++) {
				DynamoBatchObjectController dynamoBatchObjectController = this.writeBatchs.poll();
				if (dynamoBatchObjectController == null) continue;
				BatchWriteItemResult batchWriteItemResult = null;
				try {
					batchWriteItemResult = this.dynamoDB.batchWriteItem(dynamoBatchObjectController.getBatchWriteItemRequest());
				} catch (Exception e) {
					_logger.error("Batch Operation Exception: ", e);
					batchWriteItemResult = new BatchWriteItemResult();
					batchWriteItemResult.setUnprocessedItems(new ConcurrentHashMap<>());
					batchWriteItemResult.getUnprocessedItems().putAll(dynamoBatchObjectController.getBatchWriteItemRequest().getRequestItems());
				}

				this.resultWriteBatchOperation.put(dynamoBatchObjectController, batchWriteItemResult);
				synchronized (dynamoBatchObjectController) {
					dynamoBatchObjectController.notifyAll();
				}
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}	

	private void checkForTable(String tableName) {
		if(!tablesCreated.contains(tableName)) {
			DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
			TableDescription tableDescription = this.dynamoDB.describeTable(request).getTable();
			String tableStatus = tableDescription.getTableStatus();
			if (tableStatus.equals(TableStatus.ACTIVE.toString()) || tableStatus.equals(TableStatus.UPDATING.toString())) {
				tablesCreated.add(tableName);
				return;
			}
		}
	}

	private class DynamoBatchObjectController{

		BatchWriteItemRequest batchWriteItemRequest = null;

		private int size;

		private int process;

		public DynamoBatchObjectController() {
			this.batchWriteItemRequest = new BatchWriteItemRequest();
			this.batchWriteItemRequest.setRequestItems(new ConcurrentHashMap<>());
		}

		public void addItem(String tableName, WriteRequest writeRequest) {
			if(!batchWriteItemRequest.getRequestItems().containsKey(tableName)) {
				batchWriteItemRequest.getRequestItems().put(tableName, new ArrayList<>());
			}
			batchWriteItemRequest.getRequestItems().get(tableName).add(writeRequest);
			size++;
		}

		public BatchWriteItemRequest getBatchWriteItemRequest() {
			return batchWriteItemRequest;
		}

		public int getSize() {
			return size;
		}

		public void process() {
			this.process++;
		}

		public boolean isEnd() {
			return this.size == this.process;
		}
	}
}