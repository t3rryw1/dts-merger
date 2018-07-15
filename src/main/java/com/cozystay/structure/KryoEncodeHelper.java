package com.cozystay.structure;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;

class KryoEncodeHelper {
    static <T> byte[] encode(Kryo kryo, T obj) {
        ByteArrayOutputStream objStream = new ByteArrayOutputStream();
        Output objOutput = new Output(objStream);
        kryo.writeClassAndObject(objOutput, obj);
        objOutput.close();
        return objStream.toByteArray();
    }

    static <T> T decode(Kryo kryo, byte[] bytes, Class<T> tClass) {
        return (T)kryo.readClassAndObject(new Input(bytes));
    }
}
