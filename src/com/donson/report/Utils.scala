package com.donson.report

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Created by bigdataTeam on 2015/9/10 0010.
 */
object Utils {
  /**
   *   struct schema information
   *
   *   SessionID  	          string   	会话标识'
        AdvertisersID       	int	广告主ID'
        ADOrderID           	int	广告ID'
        ADCreativeID        	int	广告创意ID'
        ADPlatformProviderID	int	广告平台商ID'
        SDKVersionNumber    	string   	SDK版本号'
        AdPlatformKey       	string   	平台商Key'
        PutInModelType      	tinyint	针对广告主的投放模式，1：展示量投放；2：点击量投放
        RequestMode         	tinyint	数据请求方式（1:请求、2:展示、3:点击）'
        ADPrice             	double   	广告价格'
        ADPPPrice           	double   	平台商价格'
        RequestDate         	string   	请求时间，格式为：YYYY/mm/dd  hh:mm:ss
        Ip                  	string   	设备用户的真实IP地址'
        AppID               	string   	应用ID'
        AppName             	string   	应用名称'
        Uuid                	string   	设备唯一标识，比如imei或者androidid等'
        Device              	string   	设备型号，如HTC、iPhone'
        Client              	tinyint	设备类型
        OsVersion           	string   	设备操作系统版本，如4.0'
        Density             	string   	设备屏幕的密度
        Pw                  	int   	设备屏幕宽度'
        Ph                  	int   	设备屏幕高度'
        Long                	string   	设备所在经度'
        Lat                 	string   	设备所在纬度'
        ProvinceName        	string   	设备所在省份名称'
        CityName            	string   	设备所在城市名称'
        ISPID               	tinyint	运营商ID'
        ISPName             	string   	运营商名称'
        NetworkMannerID     	tinyint	联网方式ID'
        NetworkMannerName   	string   	联网方式名称'
        IsEffective         	char(1)	有效标识（有效指可以正常计费的）(0：无效；1：有效)
        IsBilling           	char(1)	是否收费（0：未收费；1：已收费）
        AdSpaceType         	tinyint	广告位类型（1：Banner；2：插屏；3：全屏）
        AdSpaceTypeName     	string   	广告位类型名称（Banner、插屏、全屏）
        DeviceType          	tinyint	设备类型（1：手机；2：平板）
        ProcessNode         	tinyint	流程节点（1：请求量KPI；2：有效请求；3：广告请求）
        AppType             	tinyint	应用类型ID'
        District            	string   	设备所在县名称'
        PayMode             	tinyint	针对平台商的支付模式，1：展示量投放；2：点击量投放
        IsBid               	string   	是否RTB'
        BidPrice            	double	RTB竞价价格'
        WinPrice            	double	RTB竞价成功价格'
        IsWin               	char(1)	是否竞价成功'
        Cur	                  string	币种，Values:USD|RMB等'
        Rate	                double	汇率',
        CnyWinPrice	          double	RTB竞价成功转换成人民币的价格'
        imei	                string	IMEI
        mac	                  string	MAC
        idfa	                string	IDFA
        openudid	            string	OpenUDID
        androidid	            string	AndroidID
        rtbprovince	          string	rtb渠道传过来的用户所在省份
        rtbcity	              string	rtb渠道传过来的用户所在城市
        rtbdistrict	          string	rtb渠道传过来的用户所在区域
        rtbstreet	            string	rtb渠道传过来的用户所在街道
        storeurl	            string	app市场下载地址
        realip	              string	真实ip
        IsQualityApp	         int   	是否优选，1为优选
   */
  def getSchemaInfo: StructType = {
    StructType(StructField("SessionID", StringType, true) // 0
      :: StructField("AdvertisersID", StringType, true)   // 1
      :: StructField("ADOrderID", StringType, true)       // 2
      :: StructField("ADCreativeID", StringType, true)    // 3
      :: StructField("ADPlatformProviderID", StringType, true)  // 4
      :: StructField("SDKVersionNumber", StringType, true)   // 5
      :: StructField("AdPlatformKey", StringType, true)      // 6
      :: StructField("PutInModelType", StringType, true)     // 7
      :: StructField("RequestMode", StringType, true)        // 8
      :: StructField("ADPrice", StringType, true)            // 9
      :: StructField("ADPPPrice", StringType, true)          // 10
      :: StructField("RequestDate", StringType, true)        // 11
      :: StructField("Ip", StringType, true)       // 12
      :: StructField("AppID", StringType, true)    // 13
      :: StructField("AppName", StringType, true)  // 14
      :: StructField("Uuid", StringType, true)     // 15
      :: StructField("Device", StringType, true)   // 16
      :: StructField("Client", StringType, true)   // 17
      :: StructField("OsVersion", StringType, true)// 18
      :: StructField("Density", StringType, true)  // 19
      :: StructField("Pw", StringType, true)    // 20
      :: StructField("Ph", StringType, true)    // 21
      :: StructField("Long", StringType, true)  // 22
      :: StructField("Lat", StringType, true)   // 23
      :: StructField("ProvinceName", StringType, true)  // 24
      :: StructField("CityName", StringType, true)      // 25
      :: StructField("ISPID", StringType, true)         // 26
      :: StructField("ISPName", StringType, true)       // 27
      :: StructField("NetworkMannerID", StringType, true)// 28
      :: StructField("NetworkMannerName", StringType, true)// 29
      :: StructField("IsEffective", StringType, true)      // 30
      :: StructField("IsBilling", StringType, true)        // 31
      :: StructField("AdSpaceType", StringType, true)      // 32
      :: StructField("AdSpaceTypeName", StringType, true)  // 33
      :: StructField("DeviceType", StringType, true)       // 34
      :: StructField("ProcessNode", StringType, true)      // 35
      :: StructField("AppType", StringType, true)          // 36
      :: StructField("District", StringType, true)         // 37
      :: StructField("PayMode", StringType, true)          // 38
      :: StructField("IsBid", StringType, true)            // 39
      :: StructField("BidPrice", StringType, true)         // 40
      :: StructField("WinPrice", StringType, true)         // 41
      :: StructField("IsWin", StringType, true)            // 42
      :: StructField("Cur", StringType, true)              // 43
      :: StructField("Rate", StringType, true)             // 44
      :: StructField("CnyWinPrice", StringType, true)      // 45
      :: StructField("IsShow", IntegerType)          // 以下为自定义字段标志 是否展示1：是 0：否
      :: StructField("IsClick", IntegerType)         // 是否点击 1：是 0：否
      :: StructField("IsTakeBid", IntegerType)       // 是否参与竞价 1：是 0：否
      :: StructField("IsSuccessBid", IntegerType)    // 是否竞价成功 1：是 0：否
      :: StructField("ReqDate", StringType, true)   // 请求是个格式 yyyyMMdd
      :: StructField("ReqHour", StringType, true)          // 请求是个格式 HH
      :: Nil)
  }

  /**
   *  get row RDD
   * @param rdd
   * @return
   */
  def getRowRDD(rdd: RDD[String]): RDD[Row] = {
    rdd.map(_.split(",")).map {
      prop => Row(
        prop(0).trim, prop(1).trim, prop(2).trim, prop(3).trim, prop(4).trim, prop(5).trim, prop(6).trim, prop(7).trim, prop(8).trim, prop(9).trim, prop(10).trim,
        prop(11).trim, prop(12).trim, prop(13).trim, prop(14).trim, prop(15).trim, prop(16).trim, prop(17).trim, prop(18).trim, prop(19).trim, prop(20).trim,
        prop(21).trim, prop(22).trim, prop(23).trim, prop(24).trim, prop(25).trim, prop(26).trim, prop(27).trim, prop(28).trim, prop(29).trim, prop(30).trim,
        prop(31).trim, prop(32).trim, prop(33).trim, prop(34).trim, prop(35).trim, prop(36).trim, prop(37).trim, prop(38).trim, prop(39).trim, prop(40).trim,
        prop(41).trim, prop(42).trim, prop(43).trim, prop(44).trim, prop(45).trim,

        // 展示量
        if (!prop(8).isEmpty && prop(8).trim.toInt == 2 && prop(30).trim.eq("1")) 1 else 0,
        // 点击量
        if (!prop(8).isEmpty && prop(8).trim.toInt == 3 && prop(30).trim.eq("1")) 1 else 0,
        // 参与竞价数
        if (!prop(4).isEmpty && prop(4).trim.toInt >= 100000
          && prop(30).eq("1") && prop(31).trim.eq("1") && prop(39).trim.eq("1")) 1 else 0,
        // 竞价成功数
        if (!prop(4).isEmpty && prop(4).trim.toInt >= 100000
          && prop(30).trim.eq("1") && prop(31).trim.eq("1") && prop(42).trim.eq("1")) 1 else 0,

        if (!prop(11).trim.isEmpty) formateRequestForDate(prop(11)) else NullType,
        if (!prop(11).trim.isEmpty) formateRequestForHour(prop(11)) else NullType
      )
    }
  }


  /**
   * 生成格式文件名称
   */
  def formateFileName = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())

  /**
   * 格式化时间格式为HH
   */
  def formateRequestForHour(str :String) = str.replace("-","").replace(" ","").substring(8,10)

  /**
   * 格式化时间格式为20150911-yyyyMMdd
   */
  def formateRequestForDate(str :String) = str.replace("-","").replace(" ","").substring(0,8)
}
