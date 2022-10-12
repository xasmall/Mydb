package com.xasmall.mydb;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RadomAccessFileTest {

    @Test
    public void test1() throws IOException {
        RandomAccessFile raf = new RandomAccessFile("C:\\Users\\xasmall\\Documents\\WeChat Files\\wxid_dwq3898bcut322\\FileStorage\\File\\2022-09\\1.cpp","rw");
        System.out.println("当前记录指针位置：" + raf.getFilePointer());
        byte[] buf = new byte[100];
        int len = 0;
        while((len = raf.read(buf))!=-1){
            System.out.println(new String(buf));
        }
    }
}
