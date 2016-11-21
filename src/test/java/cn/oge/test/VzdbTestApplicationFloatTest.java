package cn.oge.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import cn.oge.VzdbTestApplication;
import cn.oge.util.ConfigBeans;
import cn.oge.util.TimeUtil;
import vzdb.ByteListHolder;
import vzdb.FloatPara;
import vzdb.FloatParaListHolder;
import vzdb.FloatRealData;
import vzdb.FloatRealDataListHolder;
import vzdb.FloatSectionTableData;
import vzdb.FloatSectionTableDataListHolder;
import vzdb.FloatTagData;
import vzdb.Hdb;
import vzdb.IntListHolder;
import vzdb.MeasInfoListHolder;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = VzdbTestApplication.class)
public class VzdbTestApplicationFloatTest {

	private  Hdb rdb;
	@Autowired
	private ConfigBeans config;
	private byte[] bs = { 1, 1, 1 };
	@Before
	public void getConnection() {
		rdb = new Hdb();
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
	 * 添加点值标签名
	 * 
	 */
	@Test
	public void testFloatTag() {
		// 添加源编码
		 addFloatTag();


		// 查询源编码
		FloatPara[] floatParas = assertResult(config.getFloatTags());

		 updateFloatTag(floatParas);
		// 查询修改之后源编码

		assertResult(config.getFloatUpdateTags());
		// 删除源编码
		removeFloatTag(config.getFloatUpdateTags());

	}

	/**
	 * @param floatParas
	 * @return
	 */
	private void updateFloatTag(FloatPara[] floatParas) {
		ByteListHolder list = new ByteListHolder();
		// 修改源编码
		String[] tagUpdateFloat = config.getFloatUpdateTags().split(",");
		Assert.assertEquals("修改源编码失败返回值不为1", 1, rdb.updateFloatParas(generateParams(tagUpdateFloat, floatParas), list));
		Assert.assertArrayEquals("修改源编码失败", bs, list.value);
	}

	/**
	 * @param list
	 * @param tagUpdateFloat
	 */
	private void removeFloatTag( String tagUpdateFloat)  {
		ByteListHolder list = new ByteListHolder();
		Assert.assertEquals("删除源编码失败返回值不为1", 1, rdb.removeFloatParasByTags(tagUpdateFloat.split(","), list));
		 byte[] bs = { 1, 1, 1 };
		Assert.assertArrayEquals("删除结果不一样", bs, list.value);
	}

	/**
	 * @param list
	 */
	private void  addFloatTag() {
		// 添加源编码
		ByteListHolder list = new ByteListHolder();
		String[] tagFloat = config.getFloatTags().split(",");
		
		FloatPara[] paras = new FloatPara[3];
		Assert.assertEquals("添加源编码返回值不为1", 1, rdb.appendFloatParas(generateParams(tagFloat, paras), list));
		Assert.assertArrayEquals("添加源编码失败", bs, list.value);
	}


	/**
	 * @param codes
	 * @param paras
	 */
	private FloatPara[] generateParams(String[] codes, FloatPara[] paras) {
		for (int i = 0; i < paras.length; i++) {
			if (paras[i] == null) {

				paras[i] = new FloatPara();
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
	public void writeAndReadFloatRealDatasByTags() {
		writeFloatData();
		readFloatData();
		removeFloatHisDataByTags();
		removeFloatTag(config.getFloatTags());
		
	}

	/**
	 * @param paras
	 */
	private void readFloatData() {
		FloatTagData[] paras = getFloatTagDatas();
		FloatRealDataListHolder listHolder = new FloatRealDataListHolder();
		Assert.assertEquals("查询实时数据失败",1,rdb.readFloatRealDatasByTags(config.getFloatTags().split(","), listHolder));
		FloatRealData[] datas = listHolder.value;
		for (int i = 3; i < paras.length; i++) {
			Assert.assertEquals("查出实时数据时间不一致",paras[i].val,datas[i-3].val,0);
			Assert.assertEquals("查出实时数据时间不一致",paras[i].tm,datas[i-3].tm,0);
			
		}
	}

	/**
	 * @return
	 */
	private FloatTagData[] writeFloatData() {
		addFloatTag();
		FloatTagData[] paras = getFloatTagDatas();
		Assert.assertEquals(1,rdb.writeFloatRealDatasByTags(paras));
		return paras;
	}

	/**
	 * 
	 */
	@Test
	public void writeAndReadFloatHisDatasByTags() {
		addFloatTag();
		readHisFloatData();
		removeFloatHisDataByTags();
		removeFloatTag(config.getFloatTags());
		
	}

	/**
	 * 
	 */
	private void readHisFloatData() {
		FloatTagData[] paras = getFloatTagDatas();
		Assert.assertEquals(1,rdb.writeFloatHisDatasByTags(paras));
		int begin = rdb.getTimeT(2015, 6, 27, 8, 00, 00);
		int end= rdb.getTimeT(2015, 6, 27, 18, 00, 00);
		FloatSectionTableDataListHolder dataListHolder = new FloatSectionTableDataListHolder();
		int back=rdb.readFloatSectionTableDatasByTags(config.getFloatTags().split(","),begin,end, 0, 0, dataListHolder);
		Assert.assertEquals("查询实时数据失败",0,back);
		for (int i = 0; i < dataListHolder.value.length; i++) {
			FloatSectionTableData data = dataListHolder.value[i];
			if (data == null || data.dataList == null) continue;
			for (int j = 0; j < data.dataList.length; j++) {
				if (data.dataList[j].tm > 0) {
					Assert.assertEquals("查出实时数据时间不一致",paras[i+j*3].val,data.dataList[j].val,0);
					Assert.assertEquals("查出实时数据时间不一致",paras[i+j*3].tm,data.dataList[j].tm,0);
					
				}
			}
		}
		
	}

	

	public FloatTagData[] getFloatTagDatas() {

		FloatTagData data1 = new FloatTagData();
		data1.tag = "float1_tag";
		String tm1 = "2015-06-27 10:00:00";
		data1.tm = Integer.parseInt(TimeUtil.transStringToLong(tm1) / 1000 + "");
		data1.val = 1.1f;
		data1.flag = 1;
		FloatTagData data2 = new FloatTagData();
		data2.tag = "float2_tag";
		String tm2 = "2015-06-27 11:00:00";
		data2.tm = Integer.parseInt(TimeUtil.transStringToLong(tm2) / 1000 + "");
		data2.val = 2.2f;
		data2.flag = 1;
		
		
		FloatTagData data3 = new FloatTagData();
		data3.tag = "float3_tag";
		String tm3 = "2015-06-27 12:00:00";
		data3.tm = Integer.parseInt(TimeUtil.transStringToLong(tm3) / 1000 + "");
		data3.val = 3.3f;
		data3.flag = 1;
		
		
		FloatTagData data4 = new FloatTagData();
		data4.tag = "float1_tag";
		String tm4 = "2015-06-27 13:00:00";
		data4.tm = Integer.parseInt(TimeUtil.transStringToLong(tm4) / 1000 + "");
		data4.val = 4.4f;
		data4.flag = 1;
		
		FloatTagData data5 = new FloatTagData();
		data5.tag = "float2_tag";
		String tm5 = "2015-06-27 14:00:00";
		data5.tm = Integer.parseInt(TimeUtil.transStringToLong(tm5) / 1000 + "");
		data5.val = 5.5f;
		data5.flag = 1;
	
		

		
		FloatTagData data6 = new FloatTagData();
		data6.tag = "float3_tag";
		String tm6 = "2015-06-27 15:00:00";
		data6.tm = Integer.parseInt(TimeUtil.transStringToLong(tm6) / 1000 + "");
		data6.val = 6.6f;
		data6.flag = 1;
		FloatTagData[] paras = new FloatTagData[] { data1, data2, data3,data4,data5,data6 };
		return paras;
	}





	private FloatPara[] assertResult(String test) {
		String[] tags = test.split(",");
		FloatParaListHolder floatListHolder = new FloatParaListHolder();
		int back = rdb.floatParasByTags(tags, floatListHolder);
		Assert.assertEquals(1, back);
		FloatPara[] tagsInfos = floatListHolder.value;
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


	private  void removeFloatHisDataByTags() {
		String[] tags = config.getFloatTags().split(",");
		int startTime = rdb.getTimeT(2015, 6, 26, 7, 0, 0);
		int endTime = rdb.getTimeT(2015, 6, 28, 0, 0, 0);
		ByteListHolder retList = new ByteListHolder();
		Assert.assertEquals("删除点值数据失败", 1,rdb.removeFloatHisDatasByTags(tags, startTime, endTime, retList));
	}
	private void cleanUp(){
		removeFloatHisDataByTags();
		IntListHolder idListHolder = new IntListHolder();
		rdb.allFloatId(idListHolder, 0);

		int[] ids = idListHolder.value;

		rdb.floatInfos(ids, new MeasInfoListHolder());

		rdb.removeFloatParasByIds(ids, new ByteListHolder());
	}
}
