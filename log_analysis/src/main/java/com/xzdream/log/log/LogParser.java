package com.xzdream.log.log;

import java.util.HashMap;
import java.util.Map;

/**
 * 日志解析
 */
public class LogParser {
    public Map<String,String> parse(String log){
        Map<String,String> info = new HashMap<>();

        String[] splits = log.split("\t");

        String province = "-";
        province = splits[3];

        info.put("province",province);

        return info;
    }
}
