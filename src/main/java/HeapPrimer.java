/**
 * main.java.HeapPrimer.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

import java.io.*;
import java.lang.management.*;
import java.util.*;
import org.HdrHistogram.*;


/**
 * main.java.HeapPrimer is intended to prime the Java heap with allocation that
 * will reach and exceed the JVM's configured heap size. main.java.HeapPrimer is
 * primarily intended to "prime" the JVM's use of java heap memory at
 * startup, such that startup artifacts that can effect the timing of
 * allocation operations will not occur later in the run. main.java.HeapPrimer
 * can also be used to time object allocation operations to characterize
 * allocation operation timing behavior as the heap is filled up in
 * multiple passes.
 **/


public class HeapPrimer extends Thread {

    String versionString = "main.java.HeapPrimer version 1.1.7";
    static final int MB = 1024 * 1024;

    PrintStream log;
    InputStream inputStream;

    class HeapPrimerConfiguration {
        public boolean verbose = false;
        public boolean doSecondPass = false;
        public String logFileName = null;

        public long histLargestTrackedValue = 100L * 1000 * 1000 * 1000;
        public int histNumberOfSignificantValueDigits = 2;
        public int histPercentileTicksPerHalfDistance = 5;
        public int postPrimingDelayMsec = 3000;

        public int estimatedHeapMB;
        public int individualObjArrayLength = 500;
        public int deltaMBFromEstimatedHeapSize = 100;
        public int secondPassDeltaMBFromFirstPass = -1000;

        public int allocRateMBPerSec = 800;

        void estimateHeapSize() {
            MemoryMXBean mxbean = ManagementFactory.getMemoryMXBean();
            MemoryUsage memoryUsage = mxbean.getHeapMemoryUsage();
            estimatedHeapMB = (int) (memoryUsage.getMax() / MB);
        }

        public void parseArgs(String[] args) {
            try {
                estimateHeapSize();
                for (int i = 0; i < args.length; ++i) {
                    if (args[i].equals("")) {
                        continue; // -agent can have an empty arg if the arg list itself is otherwise empty
                    } else if (args[i].equals("-v")) {
                        config.verbose = true;
                    } else if (args[i].equals("-s")) {
                        config.doSecondPass = true;
                    } else if (args[i].equals("-i")) {
                        individualObjArrayLength = Integer.parseInt(args[++i]);
                    } else if (args[i].equals("-t")) {
                        postPrimingDelayMsec = Integer.parseInt(args[++i]);
                    } else if (args[i].equals("-a")) {
                        allocRateMBPerSec = Integer.parseInt(args[++i]);
                    } else if (args[i].equals("-d")) {
                        deltaMBFromEstimatedHeapSize = Integer.parseInt(args[++i]);
                    } else if (args[i].equals("-x")) {
                        secondPassDeltaMBFromFirstPass = Integer.parseInt(args[++i]);
                    } else if (args[i].equals("-l")) {
                        logFileName = args[++i];
                    } else {
                        throw new Exception("Invalid args");
                    }
                }
            } catch (Exception e) {
                System.err.println("Usage: java -javaagent:main.java.HeapPrimer.jar=\"[-v] [-t postPrimingDelayMsec] [-a allocRateMBPerSec] [-d deltaMBFromEstimatedHeapSize] [-s] " +
                        "[-x secondPassDeltaMBFromFirstPass] [-i individualLongArrayLength] [-l logFileName]\n");
                System.exit(1);
            }
        }
    }


    public HeapPrimer(String[] args) throws FileNotFoundException {
        this.setName("main.java.HeapPrimer");
        config.parseArgs(args);
        this.setDaemon(true);
        initLogFile();
    }

    HeapPrimerConfiguration config = new HeapPrimerConfiguration();

    public static LinkedList<Object> templList = new LinkedList<Object>();

    class SitOnSomeHeap {
        int individualLongArrayLength;
        int allocRateMBPerSec;

        public SitOnSomeHeap(int individualLongArrayLength, int allocRateMBPerSec) {
            this.individualLongArrayLength = individualLongArrayLength;
            this.allocRateMBPerSec = allocRateMBPerSec;
        }

        Object addObject(List<Object> list, Object prevObj) {
            long [] o = new long[individualLongArrayLength];
            list.add(o);
            return o;
        }

        // Compute the actual object footprint, in number of objects per MB
        long calculateObjectCountPerMB() {
            LinkedList<Object> list = new LinkedList<Object>();
            Object prevObj = null;
            long estimateObjCount = (256 * MB / (40 + (individualLongArrayLength * 8))); // rough guess

            System.gc();
            long initialUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            for (long i = 0; i < estimateObjCount; i++) {
                prevObj = addObject(list, prevObj);
            }

            long bytesUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - initialUsage;

            double bytesPerObject = ((double) bytesUsed)/estimateObjCount;
            System.gc();

            return (long)(MB / bytesPerObject);

        }

        public List [] sitOnSomeHeap(int heapMBtoSitOn, Histogram histogram, boolean verbose) {
            List [] lists = new List[heapMBtoSitOn];
            long objCountPerMB = calculateObjectCountPerMB();

            if (verbose) {
                System.out.println("\t[SitOnSomeHeap: Calculated per-object footprint is " + MB/objCountPerMB + " bytes]");
                System.out.println("\t[SitOnSomeHeap: So we'll allocate a total of " + heapMBtoSitOn * objCountPerMB + " objects]");
            }

            long lastSleepTime = System.nanoTime();
            long nanoPerMB = (allocRateMBPerSec != 0) ? (2 * 1000000000L/allocRateMBPerSec) : 0;

            for (int i = 0; i < heapMBtoSitOn; i += 10) {
                Object prevObj = null;
                // fill up a MB worth of contents in lists array slot.
                LinkedList<Object> list = new LinkedList<Object>();
                lists[i] = list;

                for (int j = 0; j < (objCountPerMB * 10); j++) {
                    long prevTime = System.nanoTime();
                    prevObj = addObject(list, prevObj);
                    long deltaTimeNs = System.nanoTime() - prevTime;
                    histogram.recordValue(deltaTimeNs, 2000000000);
                }

                for (int j = 0; j < (objCountPerMB * 10); j++) {
                    long prevTime = System.nanoTime();
                    prevObj = addObject(templList, prevObj);
                    long deltaTimeNs = System.nanoTime() - prevTime;
                    histogram.recordValue(deltaTimeNs, 2000000000);
                }

                // Throttle to allocate at roughly allocRateMBPerSec:
                while ((System.nanoTime() - lastSleepTime) < (nanoPerMB * 10)) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    }
                }
                lastSleepTime = System.nanoTime();
                if (verbose) {
                    System.out.printf(".");
                }
                templList = new LinkedList<Object>();

            }
            if (verbose) {
                System.out.printf("\n");
            }
            System.gc();

            return lists;
        }
    }

    void initLogFile() throws FileNotFoundException {
        if (config.logFileName != null) {
            log = new PrintStream(new FileOutputStream(config.logFileName), false);
        } else {
            log = System.out;
        }
    }

    public void run() {
        int heapMBtoSitOn = config.estimatedHeapMB + config.deltaMBFromEstimatedHeapSize;
        Histogram histogram = new Histogram(config.histLargestTrackedValue, config.histNumberOfSignificantValueDigits);

        if (config.verbose)
            log.printf("First pass, allocating %d MB of objects:\n", heapMBtoSitOn);
        SitOnSomeHeap sosh = new SitOnSomeHeap(config.individualObjArrayLength, config.allocRateMBPerSec);
        sosh.sitOnSomeHeap(heapMBtoSitOn, histogram,  config.verbose);
        sosh = null; // Kill the data set
        System.gc();
        if (config.verbose) {
            log.printf("Percentile distribution for first allocation pass:");
            histogram.getHistogramData().outputPercentileDistribution(log, config.histPercentileTicksPerHalfDistance, 1000.0);
            if (config.doSecondPass) {
                System.gc();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }

                heapMBtoSitOn += config.secondPassDeltaMBFromFirstPass;
                histogram.reset();

                log.printf("\n\n\nSecond pass, allocating %d MB of objects:\n", heapMBtoSitOn);
                sosh = new SitOnSomeHeap(config.individualObjArrayLength, config.allocRateMBPerSec);
                sosh.sitOnSomeHeap(heapMBtoSitOn, histogram,  config.verbose);
                sosh = null; // Kill the data set
                System.gc();
                log.printf("Percentile distribution for second allocation pass:");
                histogram.getHistogramData().outputPercentileDistribution(log, config.histPercentileTicksPerHalfDistance, 1000.0);
            }
        }
    }

    public static HeapPrimer commonMain(String[] args) {
        HeapPrimer heapPrimer = null;
        try {
            heapPrimer = new HeapPrimer(args);
            if (heapPrimer.config.verbose) {
                heapPrimer.log.print("Executing: main.java.HeapPrimer");

                for (String arg : args) {
                    heapPrimer.log.print(" " + arg);
                }
                heapPrimer.log.println("");
            }
            heapPrimer.start();
        } catch (FileNotFoundException e) {
            System.err.println("main.java.HeapPrimer: Failed to open log file.");
        }
        try {
            Thread.sleep(heapPrimer.config.postPrimingDelayMsec);
        } catch (InterruptedException e) {
        }
        return heapPrimer;
    }

    public static void premain(String argsString, java.lang.instrument.Instrumentation inst) {
        String[] args = (argsString != null) ? argsString.split(" +") : new String[0];
        HeapPrimer heapPrimer = commonMain(args);
        if (heapPrimer != null) {
            // Make sure to wait for main.java.HeapPrimer to finish before continuing on...
            try {
                heapPrimer.join();
            } catch (InterruptedException e) {
                if (heapPrimer.config.verbose) heapPrimer.log.println("main.java.HeapPrimer main() interrupted");
            }
        }
    }

    public static void main(String[] args) {
        HeapPrimer heapPrimer = commonMain(args);

        if (heapPrimer != null) {
            // The main.java.HeapPrimer thread, on it's own, will not keep the JVM from exiting. If nothing else
            // is running (i.e. we we are the main class), then keep main thread from exiting
            // until the main.java.HeapPrimer thread does...
            try {
                heapPrimer.join();
            } catch (InterruptedException e) {
                if (heapPrimer.config.verbose) heapPrimer.log.println("main.java.HeapPrimer main() interrupted");
            }
        }
    }
}

