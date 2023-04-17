package com.zhiping.wc.util;

import com.alibaba.fastjson.JSON;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
/*import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import scala.util.parsing.json.JSON;*/

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
@Slf4j
public class DingTalkUtil {
    public static void main(String[] args) throws Exception {
        Long timestamp = System.currentTimeMillis();
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/robot/send?access_token=" +
                "93ba93374af3e22a96e3fb46a336a5771f47d94e4e9ff73497621d0af53c4717"+
                "&timestamp="+timestamp+"&sign=" + getSign(timestamp));
        OapiRobotSendRequest request = new OapiRobotSendRequest();
        request.setMsgtype("text");
        OapiRobotSendRequest.Text text = new OapiRobotSendRequest.Text();
        text.setContent("测试@All");
        request.setText(text);
        OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
        //at.setAtMobiles(Arrays.asList("19916728119"));
        // isAtAll类型如果不为Boolean，请升级至最新SDK
        at.setIsAtAll(true);
        //at.setAtUserIds(Arrays.asList("nvr_3rdtr4qcz","duan643195628"));
        request.setAt(at);
        OapiRobotSendResponse response = client.execute(request);
        System.out.println("RESPONSE= " + JSON.toJSONString(response));
    }
    public static String getSign(Long timestamp) throws Exception {
        String appSecret = "SEC3842034bbebe96670e88147c6dbb4363162a880b247d226375520d428e22b0f1";

        String stringToSign = timestamp + "\n" + appSecret;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(appSecret.getBytes("UTF-8"), "HmacSHA256"));
        byte[] signData = mac.doFinal(stringToSign.getBytes("UTF-8"));
        return URLEncoder.encode(new String(Base64.encodeBase64(signData)),"UTF-8");
    }
}
