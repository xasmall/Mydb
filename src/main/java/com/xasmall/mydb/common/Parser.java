package com.xasmall.mydb.common;

import java.nio.ByteBuffer;

public class Parser {


    // 从缓冲区中读取8个字节，并返回这八个自己组成的long值
    public static long parseLong(byte[] buf){
        ByteBuffer buff = ByteBuffer.wrap(buf, 0, 8);
        // 读取此缓冲区当前位置的下八个字节，根据当前字节顺序将他们组成一个long值，然后将该位置加8
        return buff.getLong();
    }

    // 将long类型的值存到byte数组中
    public static byte[] long2Byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
