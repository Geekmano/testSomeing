package cn.oge.util;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@ConfigurationProperties("test")
@Configuration
public class ConfigBeans {
	private String floatTags;
	private String floatUpdateTags;
	private String blobTags;
	private String blobUpdateTags;
	@Value(value="${test.blobTags},${test.blobUpdateTags}")
	private String blobAlltags;
	@Value(value="${test.floatTags},${test.floatUpdateTags}")
	private String floatAlltags;
	private Rtdb rtdb=new Rtdb();
	@Data
	public static class Rtdb{
		private String host;
		private Integer port;
		private String username;
		private String password;
		
	}
	
	
}
