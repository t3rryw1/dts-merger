package com.cozystay.dts;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

class UuidBuilder {
    private List<String> uuidStrings = new ArrayList<>();

    void addValue(String str){
        uuidStrings.add(str);
    }

    String build(){
        String uuid = StringUtils.join(uuidStrings.toArray(), ':');
        return DigestUtils.shaHex(uuid);
    }

}
