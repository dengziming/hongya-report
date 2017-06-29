package com.hongya.test;

import java.util.List;

import com.hongya.etl.util.IPSeekerExt;
import com.hongya.etl.util.IPSeekerExt.RegionInfo;


public class TestIPSeekerExt {
	public static void main(String[] args) {
		IPSeekerExt ipSeekerExt = new IPSeekerExt();
		RegionInfo info = ipSeekerExt.analyticIp("114.61.94.253");
		System.out.println(info);

		List<String> ips = ipSeekerExt.getAllIp();
		for (String ip : ips) {
			System.out.println(ipSeekerExt.analyticIp(ip));
		}
	}
}
