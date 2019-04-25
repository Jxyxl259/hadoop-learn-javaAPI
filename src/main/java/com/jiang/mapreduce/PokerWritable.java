package com.jiang.mapreduce;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @description: 自定义MapReduce的数据类型
 * @author: jiangxy
 * @create: 2019-04-25 17:02
 */
public class PokerWritable implements Writable,Comparable<PokerWritable> {

    private int id;
    private String name;

    public PokerWritable() {
    }

    public PokerWritable(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PokerWritable that = (PokerWritable) o;
        return id == that.id &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return  "id:" + id + "\t\t name:" + name ;
    }

    /**
     * 序列化数据流到磁盘
     * 定义的属性的顺序要一致
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.name);
    }

    /**
     * 反序列化数据流到内存
     * 定义的属性的顺序要一致
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.name = dataInput.readUTF();
    }

    @Override
    public int compareTo(PokerWritable poker) {
        if( null == poker ){
            return 0;
        }

        // compare id
        int result = Integer.compare(this.getId(), poker.getId());
        if( result != 0){
            return result;
        }

        // compare name
        return this.getName().compareTo(poker.getName());
    }
}
