package com.lunex.test.cassandra;

import java.math.BigDecimal;
import java.text.Bidi;

import com.lunex.core.cassandra.Arithmetic;
import com.lunex.core.cassandra.Context;

public class InsertThread implements Runnable {

    private Arithmetic ctx;
    private String name;

    public InsertThread(Arithmetic ctx, String name){
        this.ctx=ctx;
        this.name = name;
    }

    public void run() {
        processCommand();
    }

    private void processCommand() {
        try {
        	System.out.println("begin " + name);
        	ctx.incre("seller_balance", 123, "amount",new BigDecimal(1));
        	ctx.commit();
        	ctx.close();
        	System.out.println("end " + name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}