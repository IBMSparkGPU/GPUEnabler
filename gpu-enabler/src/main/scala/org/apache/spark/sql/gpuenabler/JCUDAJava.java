package org.apache.spark.sql.gpuenabler; // REMOVE

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.types.StructType;
import scala.Int;
import scala.collection.immutable.Seq;
import org.apache.spark.sql.*;
import org.apache.spark.sql.gpuenabler.*;

import java.sql.Struct;

/**
 * Created by madhusudanan on 27/07/16.
 */
public class JCUDAJava { // REMOVE

    class myIterator extends JCUDAIter {

        private scala.collection.Iterator<InternalRow> inputIter;
        private StructType schema;

        public void initSchema(StructType s) {
            schema = s;
        }

        public void init(int Index, scala.collection.Iterator<InternalRow> inp) {
            inputIter = inp;
        }

        public boolean hasNext() {
            return inputIter.hasNext();
        }

        public InternalRow next() {
            UnsafeRow r = (UnsafeRow) inputIter.next();
            r.setLong(3,345);

            System.out.println("here" + r.toSeq(schema));
            return (InternalRow) r;
        }
    }

    public JCUDAIter generate(Object[] references) {
        JCUDAIter j = new myIterator();
        return j;
    }
    public JCUDAIter generateIt(StructType schema) {
        JCUDAIter j = new myIterator();
        j.initSchema(schema);
        return j;
    }
} // REMOVE
