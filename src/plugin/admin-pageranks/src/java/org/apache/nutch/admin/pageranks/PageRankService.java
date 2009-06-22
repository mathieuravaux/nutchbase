package org.apache.nutch.admin.pageranks;

import java.net.URLConnection;
import java.net.URL;
import java.io.InputStream;

/**
 * <b>PageRankService provides simple API to Google PageRank Technology</b>
 * <br>
 * PageRankService queries google toolbar webservice and returns a
 * google page rank retrieved from one of the next datacenters on the list.
 * <br>toolbarqueries.google.com
 * <br>64.233.161.100
 * <br>64.233.161.101
 * <br>64.233.177.17
 * <br>64.233.183.91
 * <br>64.233.185.19
 * <br>64.233.189.44
 * <br>66.102.1.103
 * <br>66.102.9.115
 * <br>66.249.81.101
 * <br>66.249.89.83
 * <br>66.249.91.99
 * <br>66.249.93.190
 * <br>72.14.203.107
 * <br>72.14.205.113
 * <br>72.14.255.107
 */
public class PageRankService {

    static private int dataCenterIdx = 0;

    /**
     * List of available google datacenter IPs and addresses
     */
    static final public String [] GOOGLE_PR_DATACENTER_IPS = new String[]{
                "64.233.161.100",
                "64.233.161.101",
//                "64.233.183.91",
                "66.102.1.103",
                "66.102.9.115",
//                "66.249.91.99",
                "72.14.205.113",
                "toolbarqueries.google.com",
                };

    /**
     * Default constructor
     */
    public PageRankService() {

    }

    /**
     * Must receive a domain in form of: "http://www.domain.com"
     * @param domain - (String)
     * @return PR rating (int) or -1 if unavailable or internal error happened.
     */
    public int getPR(String domain) {
    	// System.out.println("domain : " + domain);
        int result = -1;
        JenkinsHash jHash = new JenkinsHash();

        String googlePrResult = "";

        long hash = jHash.hash(("info:" + domain).getBytes());

        String url = "http://"+GOOGLE_PR_DATACENTER_IPS[dataCenterIdx]+"/search?client=navclient-auto&hl=en&"+
                "ch=6"+hash+"&ie=UTF-8&oe=UTF-8&features=Rank&q=info:" + domain;

        try {
            URLConnection con = new URL(url).openConnection();
            con.setConnectTimeout(300);
            InputStream is = con.getInputStream();
            byte [] buff = new byte[1024];
            int read = is.read(buff);
            while (read > 0) {
                googlePrResult = new String(buff, 0, read);
                read = is.read(buff);
            }
            // System.out.println("googlePrResult : '" + googlePrResult + "'");
            googlePrResult = googlePrResult.split(":")[2].trim();
            result = new Long(googlePrResult).intValue();
            
        } catch (Exception e) {
        	if( e.getClass() == java.net.SocketTimeoutException.class) {
        		System.out.println("BAD PR server : " + GOOGLE_PR_DATACENTER_IPS[dataCenterIdx]);
        	}
            e.printStackTrace();
        }

        dataCenterIdx++;
        if (dataCenterIdx == GOOGLE_PR_DATACENTER_IPS.length) {
            dataCenterIdx = 0;
        }

        // System.out.println("result : " + result);
        return result;

    }

    public static void main(String [] args) {
        long start = System.currentTimeMillis();
        PageRankService prService = new PageRankService();
        String domain = "http://www.gmail.com";
        if (args.length > 0) {
            domain = args[0];
        }
        System.out.println("Checking " + domain);
        System.out.println("Google PageRank: " + prService.getPR(domain));
        System.out.println("Took: " + (System.currentTimeMillis() - start) + "ms");
    }

}
