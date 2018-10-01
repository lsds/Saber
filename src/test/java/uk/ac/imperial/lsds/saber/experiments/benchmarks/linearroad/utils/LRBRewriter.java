package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad.utils;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.buffers.CircularQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PaddedAtomicLong;
import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.GeneratedBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LRBRewriter implements Runnable {

    private int id;
    private CircularQueryBuffer buffer;
    private int dataRange;
    private int dataLength;
    private long finalTimestamp;

    public LRBRewriter (int id, CircularQueryBuffer buffer, int dataRange, int dataLength, long finalTimestamp) {
        this.id = id;
        this.buffer = buffer;
        this.dataRange = dataRange;
        this.dataLength = dataLength;
        this.finalTimestamp = finalTimestamp;
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
        long dataToWrite = 0;

        while (true) {

            // wait until data is read
            while ((head = start.get()) == previousHead)
                ;

            tail =  end.get();
            //if ((head-previousHead +1)%dataLength != 0)
            //    throw new NullPointerException ("error: change the dataLength size");


            numberOfSlots = (int) (head-previousHead +32)/dataLength; // todo: fix this

            dataToWrite = numberOfSlots*dataLength;

            int index = this.buffer.normalise (tail);

            if (dataToWrite > (size - index)) {

                int nHead = this.buffer.normalise (head);

                for (long idx = index; idx < this.buffer.size; idx+=dataLength) {
                    this.finalTimestamp++;
                    generate((int) idx, (int) idx + dataLength, this.finalTimestamp);
                }
                for (long idx = 0; idx < nHead; idx+=dataLength) {
                    this.finalTimestamp++;
                    generate((int) idx, (int) idx + dataLength, this.finalTimestamp);
                }

            } else {

                //for (long idx = index; idx < dataToWrite+index; idx+=dataLength) {
                //    this.finalTimestamp++;
                //    generate((int) idx, (int) idx + dataLength, this.finalTimestamp);
                //}
                finalTimestamp = TheCPU.getInstance().changeTimestamps(this.buffer.getByteBuffer(), index, (int)(dataToWrite+index), dataLength, finalTimestamp);
                this.buffer.getByteBuffer().position((int)(dataToWrite+index));



            }

            int p = this.buffer.normalise (tail + dataToWrite);
            if (p <= index)
                this.buffer.incrementWraps();
            /* buffer.position(p); */
            end.lazySet(tail + dataToWrite);

            previousHead = head;

            //System.out.println("inside the writer the remainingForProcess is: " + ((CircularQueryBuffer) buffer).remainingForProcess());
        }
    }


    private void generate(int startPos, int endPos, long timestamp) {
        ByteBuffer buffer = this.buffer.getByteBuffer().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.position(startPos);
        int tempPos = startPos;

        while (buffer.position()  < endPos) {
            // change the timestamp
            buffer.putLong (timestamp);
            // skip the rest of the values
            buffer.position(buffer.position() + 24);
            tempPos += 32;
        }
    }
}
