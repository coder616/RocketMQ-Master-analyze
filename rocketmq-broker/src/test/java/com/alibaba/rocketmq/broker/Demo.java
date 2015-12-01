package com.alibaba.rocketmq.broker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


/**
 * Created by root on 2015/9/12.
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        // Calendar cal = Calendar.getInstance();
        // cal.setTimeInMillis(System.currentTimeMillis());
        // cal.add(Calendar.DAY_OF_MONTH, 1);
        // cal.set(Calendar.HOUR_OF_DAY, 0);
        // cal.set(Calendar.MINUTE, 0);
        // cal.set(Calendar.SECOND, 0);
        // cal.set(Calendar.MILLISECOND, 0);
        // System.out.println(cal.getTime());
        // removeHashMap();
        // newFile();
        // byteBufferDemo();
        System.out.println(System.lineSeparator().getBytes().length);
        readLine();
    }


    public static void updateNameServerAddressList(List<String> addrs) {
        List<String> old = new ArrayList<String>();
        boolean update = false;

        if (!addrs.isEmpty()) {
            for (int i = 0; i < addrs.size() && !update; i++) {
                if (!old.contains(addrs.get(i))) {
                    update = true;
                }
            }

            if (update) {
                // 随机打乱原来的顺序
                Collections.shuffle(addrs);
            }
        }
    }


    public static void removeHashMap() {
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "a");
        map.put(2, "a");
        map.put(3, "a");
        map.put(4, "a");
        map.put(5, "a");
        map.put(6, "a");
        map.put(7, "a");
        map.put(8, "a");
        map.put(9, "a");
        map.put(10, "a");

        Iterator<Map.Entry<Integer, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, String> next = it.next();
            if (next.getKey() % 2 == 0) {
                // map.remove(next.getKey());
                it.remove();
            }
        }
        Iterator<Map.Entry<Integer, String>> it2 = map.entrySet().iterator();
        while (it2.hasNext()) {
            System.out.println(it2.next().getKey());
        }

    }


    public static void newFile() throws Exception {
        File file = new File("E:\\demo");
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 10);
        // int offset = file.getName().getBytes().length;
        int offset = 0;
        while (true) {

            ByteBuffer slice = buffer.slice();
            slice.position(offset);
            byte[] body = "消息体".getBytes();
            if (10240 - offset > body.length + 4) {
                slice.putInt(body.length + 4);// 总自己数
                slice.put(body);
                offset += body.length + 4;
                buffer.force();
            }
            else {
                break;
            }
            System.out.println(offset);
        }
        // while (true) {
        // ByteBuffer slice = buffer.slice();
        // slice.position(offset);
        // int msgLength = slice.getInt();
        // int position = buffer.position();
        // if(msgLength>0){
        // byte[] msg = ByteBuffer.allocate(msgLength - 4).array();
        // slice.get(msg, position, msgLength-4);
        // System.out.println(new String(msg));
        // offset += msgLength;
        // }else {
        // System.out.println(msgLength);
        // break;
        // }
        //
        // }
        ByteBuffer slice = buffer.slice();
        ByteBuffer slice1 = buffer.slice();

        slice.position(offset);
        System.out.println(slice.getInt(4));
        System.out.println(slice.getInt(4));
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        System.out.println(slice.getInt());
        // channel.close();
    }


    public static int byteBufferDemo() {
        ByteBuffer buffer = ByteBuffer.allocate(1000);
        buffer.put("12".getBytes());
        buffer.flip();
        System.out.println(buffer.getInt(4));
        System.out.println(buffer.getInt());
        System.out.println(buffer.getInt());
        System.out.println(buffer.getInt());
        switch (-1) {
        case 0xAABBCCDD ^ 1880681586 + 8:
            break;
        case 0xBBCCDDEE ^ 1880681586 + 8:
            System.out.println("a");
            return 1;
        default:
            System.out.println("b");
            return 0;
        }
        System.out.println("kkkkk");
        return 0;
    }


    public static void readLine() throws Exception {
        File file = new File("E:\\demo");
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 10);
        int offset = 0;
        byte[] body = ("这里是上海" + System.lineSeparator()).getBytes();
        while (true) {
            if (offset + body.length <= 10240) {
                ByteBuffer slice = buffer.slice();
                slice.position(offset);
                slice.put(body);
                offset += body.length;
                buffer.force();
            }
            else {
                break;
            }
        }
        channel.close();
        System.out.println(file.getPath() + ".dat");
        file.renameTo(new File(file.getPath() + ".dat"));
    }

}
