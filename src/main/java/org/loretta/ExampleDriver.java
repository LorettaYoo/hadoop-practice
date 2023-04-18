package org.loretta;

import org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
    public static void main(String[] args) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("maxscore", MaxScoreMR.class,"A map/reduce program that get max score");
            exitCode = programDriver.run(args);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}
