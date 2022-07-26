package com.pingcap.poc.dbbench;

import org.apache.commons.cli.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BenchMain {
    public static void main(String[] args) throws SQLException, InterruptedException, ParseException, ClassNotFoundException {
        Config config = new Config();

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("?","help",false,"Print this usage information");
        options.addOption("h","host",true,"mysql/tidb server name/ip");
        options.addOption("p","password",true,"mysql/tidb user password");
        options.addOption("u","user",true,"mysql/tidb server user name");
        options.addOption("P","port",true,"mysql/tidb server port");
        options.addOption("d","database",true,"mysql/tidb server database");
        options.addOption("t","threads",true,"thread num");
        options.addOption("n","records num",true,"records num to insert");
        options.addOption("f","fields num",true,"fields num of table to test");
        options.addOption("b","batch num",true,"batch num of rows to insert");
        options.addOption("i","interval",true,"print progress by interval in seconds");
        options.addOption("l","field len",true,"length of each fields in char");

        //Parse the program arguments
        CommandLine commandLine = parser.parse(options, args);
        config.setHost(commandLine.getOptionValue('h', "127.0.0.1"));
        config.setUser(commandLine.getOptionValue('u', "root"));
        config.setPassword(commandLine.getOptionValue('p', ""));
        config.setDatabase(commandLine.getOptionValue('d', "test"));
        config.setPort(Integer.parseInt(commandLine.getOptionValue('P', "4000")));
        config.setCol_num(Integer.parseInt(commandLine.getOptionValue('f', "20")));
        config.setThread_num(Integer.parseInt(commandLine.getOptionValue('t', "1")));
        config.setRow_num(Integer.parseInt(commandLine.getOptionValue('n', "1000")));
        config.setBatch_num(Integer.parseInt(commandLine.getOptionValue('b', "1000")));
        config.setInterval(Integer.parseInt(commandLine.getOptionValue('i', "5")));
        config.setField_len(Integer.parseInt(commandLine.getOptionValue('l', "10")));

        String[] args1 = commandLine.getArgs();

        if (args1.length > 0){
            String cmd = args1[0];
            if (cmd.equals("prepare")) {
                prepareBench(config);
                System.exit(0);
            }
            else if (cmd.equals("run")){
                runBench(config);
                System.exit(0);
            }
        }

        print_usage(options);
    }

    public static void printHeader(){
        System.out.println("db-bench utility for POC v0.0.1");
    }

    public static void print_usage(Options options){
        printHeader();
        System.out.println("Usage: java -jar dbbench.jar [options] command ");
        System.out.println("Commands: prepare run cleanup help");
        System.out.println("General options:");
        for (Option option : options.getOptions()){
            System.out.printf("  --%s(-%s) \t %s\n", option.getLongOpt(), option.getOpt(), option.getDescription() );
        }
    }

    public static  void prepareBench(Config config) throws SQLException, ClassNotFoundException {
        printHeader();

        DBBench inst = new DBBench(config);
        inst.prepare();
        inst.close();

        System.out.printf("create table test1 done(with %d fields).\n", config.getCol_num());
    }

    public static  void runBench(Config config) throws InterruptedException {
        printHeader();
        long l1 = System.currentTimeMillis();
        System.out.printf("dbbench starting (with %d threads)... \n", config.getThread_num());
        ExecutorService svc = Executors.newFixedThreadPool(config.getThread_num() + 1);
        ArrayList<DBBench> lst = new ArrayList<>();

        for (int i = 0; i < config.getThread_num(); i++ ){
            DBBench inst = new DBBench(config);
            lst.add(inst);
            svc.execute(inst);
        }

        Monitor mon = new Monitor(lst, config.getInterval());
        svc.execute(mon);

        svc.shutdown();
        svc.awaitTermination(1, TimeUnit.HOURS);
        long l2 = System.currentTimeMillis();
        long dur = l2 - l1;
        System.out.printf("Total time used is %d ms, speed is %d rows /s\n", dur, (int)(config.getRow_num() * 1000.0 / dur));
    }

}
