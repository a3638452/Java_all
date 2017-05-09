package com.orange.service;



public class PageIndexStatisticsSpark {
	
	
	public static void main(String[] args) {
				 
	//运行页面跳转率指标的spark入口	
	new PageJumpConvertRateForAndroid1Day().pageJumpConvertRateForAndroid1Day();
    new PageJumpConvertRateForAndroid7Day().pageJumpConvertRateForAndroid7Day();
	new PageJumpConvertRateForAndroid14Day().pageJumpConvertRateForAndroid14Day();
	new PageJumpConvertRateForAndroid30Day().pageJumpConvertRateForAndroid30Day();
	new PageJumpConvertRateForIos1Day().pageJumpConvertRateForIos1Day();
	new PageJumpConvertRateForIos7Day().pageJumpConvertRateForIos7Day();
	new PageJumpConvertRateForIos14Day().pageJumpConvertRateForIos14Day();
	new PageJumpConvertRateForIos30Day().pageJumpConvertRateForIos30Day();
		
	
	}
	
}
