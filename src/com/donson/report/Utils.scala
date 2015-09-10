package com.donson.report

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Created by Administrator on 2015/9/10 0010.
 */
object Utils {
  /**
   *   struct schema information
   *   SessionID???????????	string???	会话标识'
        AdvertisersID???????	int	广告主ID'
        ADOrderID???????????	int	广告ID'
        ADCreativeID????????	int	广告创意ID'
        ADPlatformProviderID	int	广告平台商ID'
        SDKVersionNumber????	string???	SDK版本号'
        AdPlatformKey???????	string???	平台商Key'
        PutInModelType??????	tinyint	针对广告主的投放模式，1：展示量投放；2：点击量投放
        RequestMode?????????	tinyint	数据请求方式（1:请求、2:展示、3:点击）'
        ADPrice?????????????	double???	广告价格'
        ADPPPrice???????????	double???	平台商价格'
        RequestDate?????????	string???	请求时间，格式为：YYYY/mm/dd  hh:mm:ss
        Ip??????????????????	string???	设备用户的真实IP地址'
        AppID???????????????	string???	应用ID'
        AppName?????????????	string???	应用名称'
        Uuid????????????????	string???	设备唯一标识，比如imei或者androidid等'
        Device??????????????	string???	设备型号，如HTC、iPhone'
        Client??????????????	tinyint	设备类型
        OsVersion???????????	string???	设备操作系统版本，如4.0'
        Density?????????????	string???	设备屏幕的密度
        Pw??????????????????	int???	设备屏幕宽度'
        Ph??????????????????	int???	设备屏幕高度'
        Long????????????????	string???	设备所在经度'
        Lat?????????????????	string???	设备所在纬度'
        ProvinceName????????	string???	设备所在省份名称'
        CityName????????????	string???	设备所在城市名称'
        ISPID???????????????	tinyint	运营商ID'
        ISPName?????????????	string???	运营商名称'
        NetworkMannerID?????	tinyint	联网方式ID'
        NetworkMannerName???	string???	联网方式名称'
        IsEffective?????????	char(1)	有效标识（有效指可以正常计费的）(0：无效；1：有效)
        IsBilling???????????	char(1)	是否收费（0：未收费；1：已收费）
        AdSpaceType?????????	tinyint	广告位类型（1：Banner；2：插屏；3：全屏）
        AdSpaceTypeName?????	string???	广告位类型名称（Banner、插屏、全屏）
        DeviceType??????????	tinyint	设备类型（1：手机；2：平板）
        ProcessNode?????????	tinyint	流程节点（1：请求量KPI；2：有效请求；3：广告请求）
        AppType?????????????	tinyint	应用类型ID'
        District????????????	string???	设备所在县名称'
        PayMode?????????????	tinyint	针对平台商的支付模式，1：展示量投放；2：点击量投放
        IsBid???????????????	string???	是否RTB'
        BidPrice????????????	double	RTB竞价价格'
        WinPrice????????????	double	RTB竞价成功价格'
        IsWin???????????????	char(1)	是否竞价成功'
        Cur	string	币种，Values:USD|RMB等'
        Rate	double	汇率',
        CnyWinPrice	double	RTB竞价成功转换成人民币的价格'
        imei	string	IMEI
        mac	string	MAC
        idfa	string	IDFA
        openudid	string	OpenUDID
        androidid	string	AndroidID
        rtbprovince	string	rtb渠道传过来的用户所在省份
        rtbcity	string	rtb渠道传过来的用户所在城市
        rtbdistrict	string	rtb渠道传过来的用户所在区域
        rtbstreet	string	rtb渠道传过来的用户所在街道
        storeurl	string	app市场下载地址
        realip	string	真实ip
        IsQualityApp	int???	是否优选，1为优选
   */
  def getSchema: StructType = {
    StructType(StructField("SessionID", StringType)
      :: StructField("AdvertisersID", StringType)
      :: StructField("ADOrderID", StringType)
      :: StructField("ADCreativeID", StringType)
      :: StructField("ADPlatformProviderID", StringType)
      :: StructField("SDKVersionNumber", StringType)
      :: StructField("AdPlatformKey", StringType)
      :: StructField("PutInModelType", StringType)
      :: StructField("RequestMode", StringType)
      :: StructField("ADPrice", StringType)
      :: StructField("ADPPPrice", StringType)
      :: StructField("RequestDate", StringType)
      :: StructField("Ip", StringType)
      :: StructField("AppID", StringType)
      :: StructField("AppName", StringType)
      :: StructField("Uuid", StringType)
      :: StructField("Device", StringType)
      :: StructField("Client", StringType)
      :: StructField("OsVersion", StringType)
      :: StructField("Density", StringType)
      :: StructField("Pw", StringType)
      :: StructField("Ph", StringType)
      :: StructField("Long", StringType)
      :: StructField("Lat", StringType)
      :: StructField("ProvinceName", StringType)
      :: StructField("CityName", StringType)
      :: StructField("ISPID", StringType)
      :: StructField("ISPName", StringType)
      :: StructField("NetworkMannerID", StringType)
      :: StructField("NetworkMannerName", StringType)
      :: StructField("IsEffective", StringType)
      :: StructField("IsBilling", StringType)
      :: StructField("AdSpaceType", StringType)
      :: StructField("AdSpaceTypeName", StringType)
      :: StructField("DeviceType", StringType)
      :: StructField("ProcessNode", StringType)
      :: StructField("AppType", StringType)
      :: StructField("District", StringType)
      :: StructField("PayMode", StringType)
      :: StructField("IsBid", StringType)
      :: StructField("BidPrice", StringType)
      :: StructField("WinPrice", StringType)
      :: StructField("IsWin", StringType)
      :: StructField("Cur", StringType)
      :: StructField("Rate", StringType)
      :: StructField("CnyWinPrice", StringType)
      :: Nil)
  }

  /**
   *  get row RDD
   * @param rdd
   * @return
   */
  def getRowRDD(rdd: RDD[String]): RDD[Row] = {
    rdd.map(_.split(",")).map {
      prop => Row.apply(
        prop(0), prop(1), prop(2), prop(3), prop(4), prop(5), prop(6), prop(7), prop(8), prop(9), prop(10),
        prop(11), prop(12), prop(13), prop(14), prop(15), prop(16), prop(17), prop(18), prop(19), prop(20),
        prop(21), prop(22), prop(23), prop(24), prop(25), prop(26), prop(27), prop(28), prop(29), prop(30),
        prop(31), prop(32), prop(33), prop(34), prop(35), prop(36), prop(37), prop(38), prop(39), prop(40),
        prop(41), prop(42), prop(43), prop(44), prop(45),if(prop(8)==1 && prop(35).toInt>= 1 ) 1 else 0 ,
      )
    }
  }
}
