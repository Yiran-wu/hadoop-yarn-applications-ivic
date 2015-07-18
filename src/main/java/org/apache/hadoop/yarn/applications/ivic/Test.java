package org.apache.hadoop.yarn.applications.ivic;

import java.util.*;

public class Test extends Thread{
    private static final int MAX_PRIMES = 1000000;
    private static final int TEN_SECONDS = 10000;
    
    public volatile boolean finished = false;
    
    public void run() {
        int[] primes = new int[MAX_PRIMES];
        int count = 0;
        
        for (int i = 2; count < MAX_PRIMES; i++) {
            // Check to see if the timer has expired
            if (finished) {
                break;
            }
            
            boolean prime = true;
            for (int j = 0; j < count; j++) {
                if (i % primes[j] == 0) {
                    prime = false;
                    break;
                }
            }
            
            if (prime) {
                primes[count++] = i;
                System.out.println("Found prime: " + i);
            }
        }
        
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Test test = new Test();
        test.start();
        try {
            Thread.sleep(TEN_SECONDS);
        }
        catch (InterruptedException e) {
            // fall through
        }
        test.finished = true;
    }
}

