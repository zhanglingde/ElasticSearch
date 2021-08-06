package http;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

/**
 * Http 请求查询 es
 */
public class EsDemo {
    public static void main(String[] args) throws Exception{
        // 使用 Jdk 自带 Http 请求查询
        URL url = new URL("http://192.168.152.132:9200/books/_search?pretty=true");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        if (con.getResponseCode() == 200) {
            BufferedReader buffer = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String str = null;
            while ((str = buffer.readLine()) != null) {
                System.out.println(str);
            }
        }

    }
}
