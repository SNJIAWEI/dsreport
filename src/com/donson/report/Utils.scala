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
   *   SessionID???????????	string???	�Ự��ʶ'
        AdvertisersID???????	int	�����ID'
        ADOrderID???????????	int	���ID'
        ADCreativeID????????	int	��洴��ID'
        ADPlatformProviderID	int	���ƽ̨��ID'
        SDKVersionNumber????	string???	SDK�汾��'
        AdPlatformKey???????	string???	ƽ̨��Key'
        PutInModelType??????	tinyint	��Թ������Ͷ��ģʽ��1��չʾ��Ͷ�ţ�2�������Ͷ��
        RequestMode?????????	tinyint	��������ʽ��1:����2:չʾ��3:�����'
        ADPrice?????????????	double???	���۸�'
        ADPPPrice???????????	double???	ƽ̨�̼۸�'
        RequestDate?????????	string???	����ʱ�䣬��ʽΪ��YYYY/mm/dd  hh:mm:ss
        Ip??????????????????	string???	�豸�û�����ʵIP��ַ'
        AppID???????????????	string???	Ӧ��ID'
        AppName?????????????	string???	Ӧ������'
        Uuid????????????????	string???	�豸Ψһ��ʶ������imei����androidid��'
        Device??????????????	string???	�豸�ͺţ���HTC��iPhone'
        Client??????????????	tinyint	�豸����
        OsVersion???????????	string???	�豸����ϵͳ�汾����4.0'
        Density?????????????	string???	�豸��Ļ���ܶ�
        Pw??????????????????	int???	�豸��Ļ���'
        Ph??????????????????	int???	�豸��Ļ�߶�'
        Long????????????????	string???	�豸���ھ���'
        Lat?????????????????	string???	�豸����γ��'
        ProvinceName????????	string???	�豸����ʡ������'
        CityName????????????	string???	�豸���ڳ�������'
        ISPID???????????????	tinyint	��Ӫ��ID'
        ISPName?????????????	string???	��Ӫ������'
        NetworkMannerID?????	tinyint	������ʽID'
        NetworkMannerName???	string???	������ʽ����'
        IsEffective?????????	char(1)	��Ч��ʶ����Чָ���������Ʒѵģ�(0����Ч��1����Ч)
        IsBilling???????????	char(1)	�Ƿ��շѣ�0��δ�շѣ�1�����շѣ�
        AdSpaceType?????????	tinyint	���λ���ͣ�1��Banner��2��������3��ȫ����
        AdSpaceTypeName?????	string???	���λ�������ƣ�Banner��������ȫ����
        DeviceType??????????	tinyint	�豸���ͣ�1���ֻ���2��ƽ�壩
        ProcessNode?????????	tinyint	���̽ڵ㣨1��������KPI��2����Ч����3���������
        AppType?????????????	tinyint	Ӧ������ID'
        District????????????	string???	�豸����������'
        PayMode?????????????	tinyint	���ƽ̨�̵�֧��ģʽ��1��չʾ��Ͷ�ţ�2�������Ͷ��
        IsBid???????????????	string???	�Ƿ�RTB'
        BidPrice????????????	double	RTB���ۼ۸�'
        WinPrice????????????	double	RTB���۳ɹ��۸�'
        IsWin???????????????	char(1)	�Ƿ񾺼۳ɹ�'
        Cur	string	���֣�Values:USD|RMB��'
        Rate	double	����',
        CnyWinPrice	double	RTB���۳ɹ�ת��������ҵļ۸�'
        imei	string	IMEI
        mac	string	MAC
        idfa	string	IDFA
        openudid	string	OpenUDID
        androidid	string	AndroidID
        rtbprovince	string	rtb�������������û�����ʡ��
        rtbcity	string	rtb�������������û����ڳ���
        rtbdistrict	string	rtb�������������û���������
        rtbstreet	string	rtb�������������û����ڽֵ�
        storeurl	string	app�г����ص�ַ
        realip	string	��ʵip
        IsQualityApp	int???	�Ƿ���ѡ��1Ϊ��ѡ
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
