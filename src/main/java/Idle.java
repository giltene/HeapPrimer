// main.java.Idle.java
//
// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/


import java.io.*;

/**
 * main.java.Idle: A simple java program that idles for a configurable amount of time
 * and then exits. It will also exit if it's stdin pipe is severed. Useful
 * for testing and demonstrating warppers (such as HiccupMeter/jHiccup),
 * as well as for running control process tests concurrently with observed
 * applications.
 */

public class Idle extends Thread {

    class IdleConfiguration {
        public long runTimeMs = 10000;
        public boolean verbose = false;

        public void parseArgs(String[] args) {
            try {
            for (int i = 0; i < args.length; ++i) {
                if (args[i].equals("-v")) {
                    config.verbose = true;
                } else if (args[i].equals("-t")) {
                    runTimeMs = Long.parseLong(args[++i]);
                } else {
                    throw new Exception("Invalid args");
                }
            }
            } catch (Exception e) {
                System.err.println("Usage: java main.java.Idle [-v] [-h] [-t runTimeMs] ");
                System.exit(1);
            }
        }
    }

    IdleConfiguration config = new IdleConfiguration();
    
    class IdleReader extends Thread {
        IdleReader() {
            this.setDaemon(true);
            this.setName("IdleReader");
            this.start();
        }
        
        public void run() {
            // Ensure main.java.Idle exit when stdin is severed.
            try {
                while (System.in.read() >= 0) {
                }
                System.exit(1);
            } catch (Exception e) {
                System.exit(1);
            }
                    
        }
    }

    public Idle() throws FileNotFoundException {
    }

    public Idle(String[] args) throws FileNotFoundException {
        config.parseArgs(args);
    }

    public void terminate() {
        this.interrupt();
    }

    public void run() {
        new IdleReader();
        try {
            if (config.verbose)
                System.out.println("Idling for " + config.runTimeMs + "msec...");

            long startTime = System.currentTimeMillis();
            while ((config.runTimeMs == 0) || (config.runTimeMs > System.currentTimeMillis() - startTime)) {
                Thread.sleep(100); // Just wait to be interrupted/terminated or for time to expire...
            }
        } catch (InterruptedException e) {
            if (config.verbose) System.out.println("main.java.Idle terminating...");
        }
    }

    public static void main(String[] args) {
        try {
            Idle idler = new Idle(args);


            if (idler.config.verbose) {
                System.out.print("Executing: idler");

                for (String arg : args) {
                    System.out.print(" " + arg);
                }
                System.out.println("");
            }

            idler.start();
            

            try {
                idler.join();
            } catch (InterruptedException e) {
                if (idler.config.verbose) System.out.println("idler main() interrupted");
            }
            // (if you wanted idler to terminate early, call idler.terminate() )...
        } catch (FileNotFoundException e) {
            System.err.println("Failed to open log file.");
        }
    }
}

