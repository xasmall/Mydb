package com.xasmall.mydb.tm;

import com.xasmall.mydb.common.MyError;
import com.xasmall.mydb.common.Panic;
import com.xasmall.mydb.common.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{

    // xid文件头长度，8个字节的数字表示这个xid文件管理的事务的个数
    static final int LEN_XID_HEADER_LENGTH = 8;

    // 在这个xid文件中，每个事务占用1个字节表示自己的状态
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    // byte占用8位，bit是1位
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，且这个事务永远为committed状态
    public static final long SUPER_XID = 0;

    // xid文件后缀
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf,FileChannel fc){
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    private void checkXIDCounter(){

        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(MyError.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH){
            Panic.panic(MyError.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);

        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end!=fileLen){
            Panic.panic(MyError.BadXIDFileException);
        }

    }

    private void updateXiD(long xid,byte status){
        long offset = getXidPosition(xid);
        byte[] temp = new byte[XID_FIELD_SIZE];
        temp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(temp);
        try{
            fc.position(offset);
            fc.write(buf);
        }catch(IOException e) {
            Panic.panic(e);
        }

        // 强制更新到磁盘
        try {
            // force方法会将channel里面还未写入的数据全部刷新到磁盘上，false表示不会将文件元数据也刷新到磁盘上
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void incrXidCounter(){
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try{
            fc.position(0);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    @Override
    public long begin() {
        counterLock.lock();
        try{
            long xid = xidCounter + 1;
            updateXiD(xid,FIELD_TRAN_ACTIVE);
            incrXidCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    private boolean checkXID(long xid,byte status){
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public void commit(long xid) {
        updateXiD(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXiD(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
