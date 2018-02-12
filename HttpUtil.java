/**
 *
 */
package com.ceabs.tracking.utils;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * @author daniel.costa
 *
 */
public class HttpUtil {

	public String getData(InputStream inputStream, long timeout, long interval, int sizeBuffer, String charset) throws SocketTimeoutException, InterruptedException {

		byte[] data = null;
		byte[] buffer = new byte[sizeBuffer];
		
		long now = System.currentTimeMillis();
		long interationNow = now;

		int bytesRead = -1;
		while (interationNow - now < timeout || timeout == 0) {
			try {				
				bytesRead = inputStream.read(buffer);	
				
				if (bytesRead > 0) {
					data = concat(data, split(buffer, bytesRead)[0]);
				} else if (bytesRead == -1)
					break;
				Thread.sleep(interval);
				interationNow = System.currentTimeMillis();
			} catch (SocketTimeoutException se) {
				if(data != null) return new String(data, Charset.forName(charset));
			} catch (Exception e) {
				return null;
			}
		}		
		
		if(!(interationNow - now < timeout || timeout == 0)) throw new SocketTimeoutException();
		
		if(data == null && bytesRead == -1) throw new InterruptedException();

		return data != null ? new String(data, Charset.forName(charset)) : null;
	}

	public void sendFile(String path, String json, OutputStream out, String setCookies, String cookies)	throws IOException {

		DataOutputStream data = new DataOutputStream(out);
		data.writeBytes("HTTP/1.1 200 OK\r\n");

		if (path != null) {
			String contentType = "";

			if (path.contains(".js")) {
				contentType = "application/javascript; charset=utf-8";
			} else if (path.contains(".jpg") || path.contains(".png")) {
				contentType = "image/*";
			} else if (path.contains(".html")) {
				contentType = "text/html; charset=utf-8";
			}

			data.writeBytes("Content-Type: " + contentType + "\r\n");
		} else {
			data.writeBytes("Content-Type: application/json; charset=utf-8\r\n");
		}

		if (setCookies != null) {
			data.writeBytes("Set-Cookie: " + setCookies + "\r\n");
		}

		if (cookies != null) {
			data.writeBytes("Cookie: " + cookies + "\r\n");
		}

		data.writeBytes("\r\n");
		if (path != null) {
			this.writeBytesFile(new FileInputStream(path), data);
		} else {
			data.writeBytes(json);
		}

		data.flush();
	}

	private void writeBytesFile(FileInputStream fin, DataOutputStream out) throws IOException {
		byte[] buffer = new byte[1024];
		int bytesRead;

		while ((bytesRead = fin.read(buffer)) != -1) {
			out.write(buffer, 0, bytesRead);
		}
		fin.close();
	}
	
	private byte[] concat(byte[] a, byte[] b) {
		int aLen = 0;
		if(a != null) aLen = a.length;
		
		int bLen = 0;
		if(b != null) bLen = b.length;
		
		byte[] c = new byte[aLen + bLen];
		if(aLen > 0) System.arraycopy(a, 0, c, 0, aLen);
		if(bLen > 0) System.arraycopy(b, 0, c, aLen, bLen);
		return c;
	}

	public static byte[][] split(byte[] a, int size) {

		byte[] b = new byte[size];
		byte[] c = null;

		if(a.length - size > 0) c = new byte[a.length - size];

		System.arraycopy(a, 0, b, 0, size);
		if(c != null) System.arraycopy(a, size, c, 0, a.length - size);

		byte[][] d = { b, c };

		return d;
	}

	public String httpRequest(String url, String method, Map<String, String> properties, String text, String path, long timeout, long interval, int sizeBuffer, String charset)  throws SocketTimeoutException, InterruptedException {

		HttpURLConnection con = null;

		try {
			con = (HttpURLConnection) new URL(url).openConnection();
			con.setReadTimeout((int)timeout);
		} catch (IOException e) {

			return null;
		}

		try {
			con.setRequestMethod(method);
		} catch (ProtocolException e) {

			return null;
		}
		if (properties != null) {
			for (String key : properties.keySet()) {
				con.setRequestProperty(key, properties.get(key));
			}
		}
		if (text != null || path != null)
			con.setDoOutput(true);

		if (text != null) {
			DataOutputStream data = null;
			try {
				data = new DataOutputStream(con.getOutputStream());
				data.writeBytes(text);
				data.flush();
			} catch (IOException e) {

				return null;
			}
		}

		if (path != null) {
			try {
				this.writeBytesFile(new FileInputStream(path), new DataOutputStream(con.getOutputStream()));
			} catch (IOException e) {

				return null;
			}
		}

		try {
			return this.getData(con.getInputStream(), timeout, interval, sizeBuffer, charset);
		} catch (IOException e) {
			return null;
		} finally {
			con.disconnect();
		}
	}
	
	public String httpRequest(String url, String method, Map<String, String> properties, String text, String path)  throws SocketTimeoutException, InterruptedException {
		return this.httpRequest(url, method, properties, text, path, 60000, 50, 1024, "UTF-8");
	}

}
