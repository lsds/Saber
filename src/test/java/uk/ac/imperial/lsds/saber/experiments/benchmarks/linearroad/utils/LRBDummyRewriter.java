package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad.utils;

import uk.ac.imperial.lsds.saber.buffers.CircularQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PaddedAtomicLong;
import uk.ac.imperial.lsds.saber.devices.TheCPU;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

public class LRBDummyRewriter implements Runnable {

    private int id;
    private CircularQueryBuffer buffer;
    private int dataRange;
    private int dataLength;
    private AtomicInteger lock;
    private long finalTimestamp;
    private LRBRewriter parent;
    private int index;
    private long dataToWrite = 0;


    public LRBDummyRewriter(int id, CircularQueryBuffer buffer, int dataRange, int dataLength, AtomicInteger lock, LRBRewriter parent) {
        this.id = id;
        this.buffer = buffer;
        this.dataRange = dataRange;
        this.dataLength = dataLength;
        this.lock = lock;
        this.parent = parent;
    }

    @Override
    public void run() {

        TheCPU.getInstance().bind(id);
        System.out.println(String.format("[DBG] bind Worker LRBGenerator thread %2d to core %2d", id, id));

        PaddedAtomicLong end = this.buffer.getEnd();
        PaddedAtomicLong start = this.buffer.getStart();

        long head, tail;
        long previousHead = 0;
        int size = this.buffer.capacity ();
        int numberOfSlots = 0;

        long tempTimestamp;

        while (true) {


            // wait until data is read
            while (lock.get()==0)
                ;

            finalTimestamp = this.parent.finalTimestamp;
            index = this.parent.index;
            dataToWrite = this.parent.dataToWrite;

            tempTimestamp = TheCPU.getInstance().changeTimestamps(this.buffer.getByteBuffer(), this.parent.index, (int)((this.parent.dataToWrite/2+this.parent.index)), dataLength, this.parent.finalTimestamp);
            //System.out.println("dummy timestamp " + tempTimestamp);


            lock.decrementAndGet();
        }
    }
}
