package com.lunex.test.cassandra;

import com.lunex.core.cassandra.Airthmetic;

public class MergeThread implements Runnable {

    private Airthmetic ctx;
    private String name;

    public MergeThread(Airthmetic ctx, String name){
        this.ctx=ctx;
        this.name = name;
    }

    public void run() {
        processCommand();
    }

    private void processCommand() {
        try {
        	System.out.println("begin " + name);
        	ctx.merge("seller_balance", 123, "amount");
        	ctx.close();
        	System.out.println("end " + name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}