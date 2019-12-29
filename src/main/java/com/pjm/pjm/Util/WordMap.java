package com.pjm.pjm.Util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WordMap {

    public static Map<String, Object> wordmap = new ConcurrentHashMap<>();

    /**
     * 将用户和sessionId存入map
     *
     * @param key
     * @param value
     */
    public static void settomap(String key, String value) {
        wordmap.put(key, value);
    }

    /**
     * 获取loginUsers
     *
     * @return
     */
    public static Map<String, Object> getmap() {
        return wordmap;
    }

    /**
     * 根据sessionId移除map中的值
     *
     * @param sessionId
     */
    public static void remove(String sessionId) {
        for (Map.Entry<String, Object> entry : wordmap.entrySet()) {
            if (sessionId.equals(entry.getValue())) {
                wordmap.remove(entry.getKey());
                break;
            }
        }
    }

    /**
     * 判断用户是否在loginusers中
     *
     * @param loginId
     * @param sessionId
     * @return
     */
    public static boolean exist(String loginId, String sessionId) {
        System.out.println(loginId+"+"+sessionId+">>>>>>>"+wordmap);
        boolean bool = wordmap.containsKey(loginId);
        boolean bool1 = sessionId.equals(wordmap.get(loginId));
        System.out.println(bool+"+"+bool1+"+"+sessionId+"+"+wordmap.get(loginId)+"+"+loginId);
        return bool && bool1;
    }

}