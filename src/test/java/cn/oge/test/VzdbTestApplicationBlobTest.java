package cn.oge.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import cn.oge.VzdbTestApplication;
import cn.oge.util.ConfigBeans;
import cn.oge.util.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import vzdb.BlobData;
import vzdb.BlobDataHolder;
import vzdb.BlobDataListHolder;
import vzdb.BlobPara;
import vzdb.BlobParaListHolder;
import vzdb.BlobTagData;
import vzdb.ByteListHolder;
import vzdb.Hdb;
import vzdb.IntListHolder;
import vzdb.MeasInfoListHolder;
import vzdb.RecordInfo;
import vzdb.RecordInfoListHolder;
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = VzdbTestApplication.class)
public class VzdbTestApplicationBlobTest {
	private static Hdb rdb;
	
	private int count =0;
	private byte[] bs = { 1, 1, 1 };
	@Autowired
	private ConfigBeans config;
	
	@Before
	public  void getConnection() {
		count=count+1;
		rdb = new Hdb();
		log.info("config: {}",config);
		int i = rdb.connect(config.getRtdb().getHost(),config.getRtdb().getPort()
				, config.getRtdb().getUsername(),config.getRtdb().getPassword());
		if (i != vzdb.ConnectSucceed.value) {
			throw new RuntimeException("VZDB数据库连接失败，错误码：" + i);
		}
	}
	
	@After
	public void destory() {
		cleanUp();
		if (rdb != null) {
			rdb.disconnect();
		}
	}

	/**
	 * @param floatParas
	 * @return
	 */
	private void updateBlobTag(BlobPara[] floatParas) {
		ByteListHolder list = new ByteListHolder();
		// 修改源编码
		String[] tagUpdateFloat = config.getBlobTags().split(",");
		Assert.assertEquals("修改源编码返回值back 不是1", 1,rdb.updateBlobParas(generateParams(tagUpdateFloat, floatParas), list) );
		Assert.assertArrayEquals("修改源编码失败", bs, list.value);
	}

	/**
	 * @param tagUpdateFloat
	 */
	private void removeBlobTag( String tagUpdateFloat)  {
		ByteListHolder list = new ByteListHolder();
		Assert.assertEquals("删除源编码失败", 1, rdb.removeBlobParasByTags(tagUpdateFloat.split(","), list));

		// 查询删除后得源编码

		byte[] bs = { 1, 1, 1 };
		Assert.assertArrayEquals("删除结果不一样", bs, list.value);
	}

	/**
	 * @param list
	 */
	private void  addBlobTag() {
		// 添加源编码
		ByteListHolder list = new ByteListHolder();
		String[] tagFloat = config.getBlobTags().split(",");
		int back =rdb.appendBlobParas(generateParams(tagFloat, new BlobPara[tagFloat.length]), list);
		Assert.assertEquals("添加源编码失败返回值不为1", 1, back);
		Assert.assertArrayEquals("添加源编码失败", bs, list.value);
	}

	/**
	 * 添加块数据的标签名
	 * 
	 * @param hdb
	 * @return
	 */
	@Test
	public void testBlobTag() {
				// 添加源编码
//				addBlobTag();

				 // 查询源编码
				BlobPara[] floatParas = assertBlobResult(config.getBlobTags());

				updateBlobTag(floatParas);
				// 查询修改之后源编码

				assertBlobResult(config.getBlobUpdateTags());
				// 删除源编码
				removeBlobTag(config.getBlobUpdateTags());

	}

	/**
	 * @param codes
	 * @param paras
	 */

	private BlobPara[] generateParams(String[] codes, BlobPara[] paras) {
		for (int i = 0; i < paras.length; i++) {
			if (paras[i] == null) {

				paras[i] = new BlobPara();
			}
			paras[i].tag = codes[i];
			paras[i].desc = codes[i];
			paras[i].isScan = 1;
			paras[i].isArchive = 1;
			paras[i].isAlarm = 0;
		}
		return paras;
	}

	
	/**
	 * 写入点值的实时数据
	 *
	 * @param hdb
	 * @return
	 */
	@Test
	public void writeAndReadBolbRealDatas() {
		writeBlobData();
		readRealBlobData();
		removeBlobDatasByTags();
		removeBlobTag(config.getBlobTags());
		
	}

	/**
	 * @param paras
	 */
	private void readRealBlobData() {
		BlobTagData[] paras = getBlobTagDatas();
		BlobDataListHolder listHolder = new BlobDataListHolder();
		Assert.assertEquals("查询实时数据失败",1,rdb.readBlobRealDatasByTags(config.getBlobTags().split(","), listHolder));
		BlobData[] datas = listHolder.value;
		for (int i = 3; i < paras.length; i++) {
			Assert.assertArrayEquals("查出实时数据时间不一致",paras[i].val,datas[i-3].val);
			Assert.assertEquals("查出实时数据时间不一致",paras[i].tm,datas[i-3].tm,0);
			
		}
	}

	/**
	 * @return
	 */
	private BlobTagData[] writeBlobData() {
		addBlobTag();
		BlobTagData[] paras = getBlobTagDatas();
		Assert.assertEquals(1,rdb.writeBlobDatasByTags(paras));
		return paras;
	}
	@Test
	public void writeAndReadBlobInfo() {
		writeBlobData();
		readBlobDataTime();
		removeBlobDatasByTags();
		removeBlobTag(config.getBlobTags());
	}
	@Test
	public void writeAndReadBlobData() {
		writeBlobData();
		readBlobRTDataHistory();
		removeBlobDatasByTags();
		removeBlobTag(config.getBlobTags());
		
	}
	
	public void readBlobRTDataHistory() {
		BlobTagData[] blobTagDatas=getBlobTagDatas();
		BlobDataHolder dataHolder = new BlobDataHolder();
		int time = rdb.getTimeT(2015, 7, 27, 8, 00, 00);
		Assert.assertEquals(1, rdb.readBlobHisDataByTag("blob1_tag", time, dataHolder));
		BlobData data = dataHolder.value;
		Assert.assertEquals("查询块数据时间不一致", data.tm,blobTagDatas[0].tm,0);
		Assert.assertArrayEquals("查询块数据数据不一致", data.val,blobTagDatas[0].val);
		
	}
	public void readBlobDataTime() {
		BlobTagData[] blobTagDatas=getBlobTagDatas();
		RecordInfoListHolder blobList = new RecordInfoListHolder();
		int begin = rdb.getTimeT(2015, 7, 26, 00, 00, 00);
		int end = rdb.getTimeT(2015, 7, 28, 00, 00, 00);
		Assert.assertEquals(1,rdb.readBlobHisInfoByTag("blob1_tag", begin, end, blobList));
		
		RecordInfo[] data = blobList.value;
		for (int i = 0; i < data.length; i++) {
			Assert.assertEquals("查询块数据时间不一致", data[i].tm,blobTagDatas[i*3].tm,0);
		}
	}


	public BlobTagData[] getBlobTagDatas() {
		BlobTagData data1 = new BlobTagData();
		data1.tag = "blob1_tag";
		String tm1 = "2015-07-27 08:00:00";
		data1.tm = Integer.parseInt(TimeUtil.transStringToLong(tm1) / 1000 + "");
		data1.val = new byte[] { 17, 52, 124, 27, -93, 71, 73, 99, -8, -49, 50, -64, -127, 47, 6, -56, -97, -37, 4, -11,
				-66, 55, -127, -29, 66, 67, 44, -7, 101, -120, 19, 65, 77, 81, -12, -89, 41, -102, 46, 107, 6, 87, 101,
				-124, -43, -95, 70, 72, -85, -47, 28, 95, -41, 52, -121, -87, 118, 11, -116, 93, -33, 2, -111, 117,
				-115, -111, -71, -55, 24, 127, -12, 90, -62, 114, 107, 75, 76, 107, -36, 10, -79, 49, -83, 112, -65,
				-87, 9, 52, 119, -101, -96, -105, 113, 107, 4, -20, 107, -115, -124, -42, 109, -16, -4, 96, 27, -24,
				-75, 51, -59, -64, 35, -90, 8, -18, -40, 22, -89, -114, -73, 69, -79, -91, 25, -102, -97, 54, -125,
				-101, 117, 59, -84, 77, 109, -121, 75, 61, -38, -93, -20, 66, 123, -76, -77, 55, -121, -9, 21, 115, 108,
				-23, -41, 1, 89, -41, 59, 0, 3, 59, -62, -22, 86, 71, -52, 24, 98, -127, -72, 59, 22, -56, 117, -19, 4,
				-19, 7, -99, 96, -17, -34, 25, 11, -14 };
		data1.flag = 1;

		BlobTagData data2 = new BlobTagData();
		data2.tag = "blob2_tag";
		String tm2 = "2015-07-27 09:00:00";
		data2.tm = Integer.parseInt(TimeUtil.transStringToLong(tm2) / 1000 + "");
		data2.val = new byte[] { 12, -106, 47, -122, 97, -38, 4, 87, -60, 22, -70, -30, -2, -28, -31, -48, 124, 55, 28,
				118, -66, 35, 16, 80, 50, 2, 9, -77, -36, -16, -4, -117, 27, -12, 2, 70, 98, -32, -9, -111, 8, 94, -24,
				-114, 83, -65, -36, 81, 28, -28, -127, -26, 127, 61, -32, -74, 108, 20, -62, -108, 39, 46, -123, 122,
				-94, -84, -58, 104, -76, 11, 27, 13, 111, -19, 49, -40, -78, 126, 12, -78, -22, 122, 1, -111, 94, -80,
				106, 48, 22, 51, -74, -114, 69, 92, -29, 113, -56, -115, 25, 7, -19, 102, -34, -80, -33, -19, -115, 5,
				-58, -29 };
		data2.flag = 1;

		BlobTagData data3 = new BlobTagData();
		data3.tag = "blob3_tag";
		String tm3 = "2015-07-27 10:00:00";
		data3.tm = Integer.parseInt(TimeUtil.transStringToLong(tm3) / 1000 + "");
		data3.val = new byte[] { -101, -35, 99, 1, -17, -6, 3, 0, -101, 19, 0, 0, 120, -100, -19, -42, 105, 87, 8, 124,
				-65, -59, -15, -3, -5, 19, 41, 17, 17, -119, -120, 72, 68, 68, 74, 68, 68, 34, 34, 82, 34, 34, 115, 68,
				100, -120, 92, 17, 25, 34, 34, 17, 17, 25, 34, 34, 67, 68, 100, -120, -120, -24, 66, 68, 100, -120,
				-120, -56, 16, 17, -119, -50, 89, -25, 37, -100, 7, -25, 94, 103, -83, -3, 121, 5, -33, 39, 123, -83,
				-115, -43, 2, 44, -5, 81, -84, 54, 46, -72, 43, -89, -86, -29, 127, 88, 45, 7, 102, 84, 23, -60, -83,
				18, 60, -44, 84, -48, 94, -89, -48, 71, -89, 26, 22, 68, 84, -61, -111, 122, -43, -15, 114, 115, 117,
				-24, -21, 107, 96, -16, 54, 13, -124, 52, -87, -127, 51, 59, 107, -96, -60, -88, 38, 90, -18, -87, 9,
				-113, 86, -102, 88, 31, -81, -119, 43, -90, -75, 80, 126, -88, 22, 58, -104, 107, -63, 39, 73, 11, -37,
				58, 105, 35, -5, -92, 54, -86, 89, -43, -122, -51, -103, -38, -104, -43, 93, 7, -5, -50, -21, -32, -79,
				93, 29, -44, -71, 92, 7, 14, 125, -21, 98, 81, 70, 93, 36, 57, -22, -30, -11, 13, 93, 24, 56, -41, -61,
				-48, -20, 122, 8, 29, 90, 31, -25, -18, -43, -57, -25, 17, 122, 48, 121, -88, -121, -47, -93, 26, 32,
				-30, 73, 3, 92, -13, 106, -120, -118, -25, 13, -47, 105, -126, 62, 38, 23, -22 };
		data3.flag = 1;
		
		BlobTagData data4 = new BlobTagData();
		data4.tag = "blob1_tag";
		String tm4 = "2015-07-27 11:00:00";
		data4.tm = Integer.parseInt(TimeUtil.transStringToLong(tm4) / 1000 + "");
		data4.val = new byte[] { 66, 52, 123, 27, -93, 71, 73, 99, -8, -49, 50, -64, -127, 47, 6, -56, -97, -37, 4, -11,
				-66, 55, -127, -29, 66, 67, 44, -7, 101, -120, 19, 65, 77, 81, -12, -89, 41, -102, 46, 107, 6, 87, 101,
				-124, -43, -95, 70, 72, -85, -47, 28, 95, -41, 52, -121, -87, 118, 11, -116, 93, -33, 2, -111, 117,
				-115, -111, -71, -55, 24, 127, -12, 90, -62, 114, 107, 75, 76, 107, -36, 10, -79, 49, -83, 112, -65,
				-87, 9, 52, 119, -101, -96, -105, 113, 107, 4, -20, 107, -115, -124, -42, 109, -16, -4, 96, 27, -24,
				-75, 51, -59, -64, 35, -90, 8, -18, -45, 22, -89, -114, -73, 69, -79, -91, 25, -102, -97, 54, -125,
				-101, 117, 59, -84, 77, 109, -121, 75, 61, -38, -93, -20, 66, 123, -76, -77, 55, -121, -9, 21, 115, 108,
				-23, -41, 1, 89, -41, 59, 0, 3, 59, -62, -22, 86, 71, -52, 24, 98, -127, -72, 59, 22, -56, 117, -19, 4,
				-19, 7, -99, 96, -17, -34, 25, 11, -14 };
		data4.flag = 1;
		
		BlobTagData data5 = new BlobTagData();
		data5.tag = "blob2_tag";
		String tm5 = "2015-07-27 12:00:00";
		data5.tm = Integer.parseInt(TimeUtil.transStringToLong(tm5) / 1000 + "");
		data5.val = new byte[] { 12, -106, 47, -122, 97, -38, 41, 87, -60, 22, -70, -30, -23, -28, -31, -48, 124, 55, 28,
				118, -66, 35, 16, 80, 50, 2, 9, -77, -36, -16, -42, -117, 27, -12, 2, 70, 98, -32, -9, -111, 8, 94, -24,
				-114, 83, -65, -36, 81, 28, -28, -127, -26, 127, 61, -32, -74, 108, 20, -62, -108, 39, 46, -123, 122,
				-94, -84, -58, 104, -76, 11, 27, 13, 111, -19, 49, -40, -78, 126, 12, -78, -22, 122, 1, -111, 94, -80,
				106, 48, 22, 51, -74, -114, 69, 92, -29, 113, -56, -115, 25, 7, -19, 102, -34, -80, -33, -19, -115, 5,
				-58, -29 };
		data5.flag = 1;
		
		BlobTagData data6 = new BlobTagData();
		data6.tag = "blob3_tag";
		String tm6 = "2015-07-27 13:00:00";
		data6.tm = Integer.parseInt(TimeUtil.transStringToLong(tm6) / 1000 + "");
		data6.val = new byte[] { -101, -35, 99, 123, -17, -65, 32, 0, -101, 19, 0, 0, 120, -100, -19, -42, 105, 87, 86, 124,
				-65, -59, -15, -3, -5, 19, 41, 17, 17, -119, -120, 72, 68, 68, 74, 68, 68, 34, 34, 82, 34, 34, 115, 68,
				100, -120, 92, 17, 25, 34, 34, 17, 17, 25, 34, 34, 67, 68, 100, -120, -120, -24, 66, 68, 100, -120,
				-120, -56, 16, 17, -119, -50, 89, -25, 37, -100, 7, -25, 94, 103, -83, -3, 121, 5, -33, 39, 123, -83,
				-115, -43, 2, 44, -5, 81, -84, 54, 46, -72, 43, -89, -86, -29, 127, 88, 45, 7, 102, 84, 23, -60, -83,
				18, 60, -44, 84, -48, 94, -89, -48, 71, -89, 26, 22, 68, 84, -61, -111, 122, -43, -15, 114, 115, 117,
				-24, -21, 107, 96, -16, 54, 13, -124, 52, -87, -127, 51, 59, 107, -96, -60, -88, 38, 90, -18, -87, 9,
				-113, 86, -102, 88, 31, -81, -119, 43, -90, -75, 80, 126, -88, 22, 58, -104, 107, -63, 39, 73, 11, -37,
				58, 105, 35, -5, -92, 54, -86, 89, -43, -122, -51, -103, -38, -104, -43, 93, 7, -5, -50, -21, -32, -79,
				93, 29, -44, -71, 92, 7, 14, 125, -21, 98, 81, 70, 93, 36, 57, -22, -30, -11, 13, 93, 24, 56, -41, -61,
				-48, -20, 122, 8, 29, 90, 31, -25, -18, -43, -57, -25, 17, 122, 48, 121, -88, -121, -47, -93, 26, 32,
				-30, 73, 3, 92, -13, 106, -120, -118, -25, 13, -47, 105, -126, 62, 38, 23, -22 };
		data6.flag = 1;

		return new BlobTagData[] { data1, data2, data3 ,data4,data5,data6};
	}



	private BlobPara[] assertBlobResult(String test) {
		String[] tags = test.split(",");
		BlobParaListHolder floatListHolder = new BlobParaListHolder();
		Assert.assertEquals(1, rdb.blobParasByTags(tags, floatListHolder));
		BlobPara[] tagsInfos = floatListHolder.value;
		Assert.assertEquals("取出得标签和更改得标签不一致", tags.length, tagsInfos.length);
		StringBuffer sb = new StringBuffer(100);
		for (int i = 0; i < tagsInfos.length; i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(tagsInfos[i].tag);
		}
		Assert.assertEquals("添加源编码与查出得结果不符合", test, sb.toString());
		return tagsInfos;
	}

	public  void removeBlobDatasByTags(){
		String[] tagStrings =config.getBlobTags().split(",");
		int start = rdb.getTimeT(2015, 7, 26, 0, 0, 0);
		int end = rdb.getTimeT(2015, 7, 28, 0, 0, 0);
		Assert.assertEquals(1, rdb.removeBlobDatasByTags(tagStrings, start, end, new ByteListHolder()));
	}

		
	private void cleanUp(){
		removeBlobDatasByTags();
		IntListHolder idListHolder = new IntListHolder();
		rdb.allBlobId(idListHolder, 0);

		int[] ids = idListHolder.value;

		rdb.blobInfos(ids, new MeasInfoListHolder());

		rdb.removeBlobParasByIds(ids, new ByteListHolder());
	}
}
