
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

/**
 * @author daniel.costa
 *
 */
public class RedisConnector {

	private static final Logger _logger = LoggerFactory.getLogger(RedisConnector.class);	
	
	private ConcurrentMap<Integer, Queue<Jedis>> jedisPoolReader;
	private ConcurrentMap<Integer, Integer> readersSize;
	
	private ConcurrentMap<Integer, Queue<Jedis>> jedisPoolWriter;
	private ConcurrentMap<Integer, Integer> writersSize;
	
	private String connectionStringRead;
	private String connectionStringWrite;

	public RedisConnector(String connectionStringRead, String connectionStringWrite) {
		this.connectionStringRead = connectionStringRead;
		this.connectionStringWrite = connectionStringWrite;
		this.jedisPoolReader = new ConcurrentHashMap<>();
		this.readersSize = new ConcurrentHashMap<>();
		this.jedisPoolWriter = new ConcurrentHashMap<>();
		this.writersSize = new ConcurrentHashMap<>();
	}

	private Jedis getReader(int db) {

		Jedis jedis = null;
		while(jedis == null) {
			if(!this.jedisPoolReader.containsKey(db)) {
				this.jedisPoolReader.put(db, new ConcurrentLinkedQueue<>());
				this.readersSize.put(db, 1);
				jedis = new Jedis(this.connectionStringRead);
				jedis.select(db);
			} else {
				jedis = this.jedisPoolReader.get(db).poll();				
				if(jedis == null) {
					if(this.readersSize.get(db) < 10) {
						this.readersSize.put(db, (this.readersSize.get(db) + 1));
						jedis = new Jedis(this.connectionStringRead);
						jedis.select(db);
						break;
					}
					try {
						synchronized (this.jedisPoolReader.get(db)) { this.jedisPoolReader.get(db).wait(5); }
					} catch (InterruptedException e) {}
				}
			}			
		}

		return jedis;
	}

	private void returnReader(Jedis jedis, int db) {
		
		if(jedis != null) {
			synchronized (this.jedisPoolReader.get(db)) {
				this.jedisPoolReader.get(db).offer(jedis);
				this.jedisPoolReader.get(db).notify();
			}
		} else {
			this.readersSize.put(db, (this.readersSize.get(db) - 1));
		}
	}

	private Jedis getWriter(int db) {

		Jedis jedis = null;
		while(jedis == null) {
			if(!this.jedisPoolWriter.containsKey(db)) {
				this.jedisPoolWriter.put(db, new ConcurrentLinkedQueue<>());
				this.writersSize.put(db, 1);
				jedis = new Jedis(this.connectionStringWrite);
				jedis.select(db);
			} else {
				jedis = this.jedisPoolWriter.get(db).poll();
				if(jedis == null) {
					if(this.writersSize.get(db) < 10) {
						this.writersSize.put(db, (this.writersSize.get(db) + 1));
						jedis = new Jedis(this.connectionStringWrite);
						jedis.select(db);
						break;
					}
					try {
						synchronized (this.jedisPoolWriter.get(db)) { this.jedisPoolWriter.get(db).wait(5); }
					} catch (InterruptedException e) {}
				}
			}
		}

		return jedis;
	}

	private void returnWriter(Jedis jedis, int db) {

		if(jedis != null) {
			synchronized (this.jedisPoolWriter.get(db)) {
				this.jedisPoolWriter.get(db).offer(jedis);
				this.jedisPoolWriter.get(db).notify();
			}
		} else {
			this.writersSize.put(db, (this.writersSize.get(db) - 1));
		}
	}

	public String get(String key, int db) {
		
		Jedis jedis = getReader(db);

		try {
			return jedis.get(key);
		} catch (Exception e) {
			_logger.error("Redis Error", e);
			jedis = null;
			return null;
		} finally {
			returnReader(jedis, db);
		}
	}
	
	public byte[] get(byte[] key, int db) {
		
		Jedis jedis = getReader(db);

		try {
			return jedis.get(key);
		} catch (Exception e) {
			_logger.error("Redis Error", e);
			jedis = null;
			return null;
		} finally {
			returnReader(jedis, db);
		}
	}
	
	public void set(String key, String value, int db) {

		Jedis jedis = getWriter(db);

		try {
			jedis.set(key, value);
		} catch (Exception e) {
			_logger.error("Redis Error", e);
			jedis = null;
			return;
		} finally {
			returnWriter(jedis, db);
		}
	}
	
	public void set(byte[] key, byte[] value, int db) {
		
		Jedis jedis = getWriter(db);

		try {
			jedis.set(key, value);
		} catch (Exception e) {
			_logger.error("Redis Error", e);
			jedis = null;
			return;
		} finally {
			returnWriter(jedis, db);
		}
	}

}
