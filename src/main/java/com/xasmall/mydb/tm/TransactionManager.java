package com.xasmall.mydb.tm;

import com.xasmall.mydb.common.MyError;
import com.xasmall.mydb.common.Panic;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    /**
     * 开启一个新事务
     * @return  新事务的xid
     */
    long begin();

    /**
     * 提交一个事务
     * @param xid 事务的xid
     */
    void commit(long xid);

    /**
     * 撤销一个事务
     * @param xid 事务的xid
     */
    void abort(long xid);

    /**
     * 判断事务是否正在进行
     * @param xid 事务的xid
     * @return  true：事务正在进行 false：事务处于其他状态
     */
    boolean isActive(long xid);

    /**
     * 判断事务是否已提交
     * @param xid 事务的xid
     * @return true：事务已经提交 false：事务处于其他状态
     */
    boolean isCommitted(long xid);

    /**
     * 判断事务是否已经撤销
     * @param xid 事务的xid
     * @return true：事务已经撤销 false：事务处于其他状态
     */
    boolean isAborted(long xid);

    /**
     * 关闭tm
     */
    void close();

    // 创建一个xid文件并创建TM
    public static TransactionManagerImpl create(String path){
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try{
            // 文件不存在则创建，存在则不创建并返回false，文件路径必须存在才可创建路径下的文件
            if(!f.createNewFile()){
                Panic.panic(MyError.FileExistsException);
            }
        }catch (IOException e){
            Panic.panic(e);
        }
        if(!f.canRead()||!f.canWrite()){
            Panic.panic(MyError.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        }catch (IOException e){
            Panic.panic(e);
        }
        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try{
            fc.position(0);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf,fc);
    }

    // 从一个已有的xid文件创建TM
    public static TransactionManagerImpl open(String path){
        File f = new File(path + TransactionManagerImpl.SUPER_XID);
        if(!f.exists()){
            Panic.panic(MyError.FileNotExistsException);
        }
        if(!f.canRead()||!f.canWrite()){
            Panic.panic(MyError.FileCannotRWException);
        }
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try{
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        }catch (IOException e){
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf,fc);
    }
}
