package com.cozystay;

import com.beust.jcommander.*;
import com.cozystay.command.UdpSender;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

public class CLI {
    @Parameter(names = {"--help", "-h"}, description = "view usage", order = 0)
    private boolean help;
    @Parameter(names = {"--version"}, description = "view version", order = 1)
    private boolean version;

    @Parameters(separators = "=", commandDescription = "you got following status operations")
    public static class CommandStatus {
        @Parameter(names = {"--address", "-a"}, required = true, description = "udp-server address", order = 0)
        private String address = "";
        @Parameter(names = {"--port", "-p"}, required = true, description = "udp-server port", order = 1)
        private int port = 0;
        @Parameter(names =  {"--view"}, description = "view status of sync queue", order = 2)
        private boolean view = false;
    }
    @Parameters(separators = "=", commandDescription = "you got following syncTask operations")
    public static class CommandTask {
        @Parameter(required = true, description = "[taskId]")
        private String taskId = "";
        @Parameter(names = {"--address", "-a"}, required = true, description = "udp-server address", order = 0)
        private String address = "";
        @Parameter(names = {"--port", "-p"}, required = true, description = "udp-server port", order = 1)
        private int port = 0;
        @Parameter(names =  {"--view"}, description = "view syncTask by id", order = 2)
        private boolean view = false;
        @Parameter(names =  {"--remove"}, description = "remove syncTask from it's queue", order = 4)
        private boolean remove = false;
    }
    @Parameters(separators = "=", commandDescription = "you got following delete operations")
    public static class CommandDelete {
        @Parameter(names = {"--address", "-a"}, required = true, description = "udp-server address", order = 0)
        private String address = "";
        @Parameter(names = {"--port", "-p"}, required = true, description = "udp-server port", order = 1)
        private int port = 0;
        @Parameter(names =  {"--fail"}, description = "view status of sync queue", order = 2)
        private boolean failed = false;
    }

    public void run(CLI cli, String[] argv) throws IOException {
        Properties prop = new Properties();
        prop.load(CLI.class.getResourceAsStream("/db-config.properties"));

        CommandStatus status = new CommandStatus();
        CommandTask task = new CommandTask();
        CommandDelete del = new CommandDelete();

        JCommander jCommander = JCommander.newBuilder()
                .addObject(cli)
                .addCommand("status", status)
                .addCommand("task", task)
                .addCommand("del", del)
                .build();

        jCommander.parse(argv);

        if(help){
            jCommander.setProgramName(prop.getProperty("cli.name"));
            jCommander.usage();
            return;
        }

        if(version){
            JCommander.getConsole().println(prop.getProperty("cli.version"));
            return;
        }


        if(!status.address.equals("") && status.port != 0){
            UdpSender client = UdpSender.start(status.address, status.port);

            if(status.view){
                client.requestGlobalStatus();
            }
        }

        if(!task.address.equals("") && task.port != 0){
            UdpSender client = UdpSender.start(task.address, task.port);

            if(task.taskId.length() <= 0){
                return;
            }

            if(task.view){
                client.requestTaskDetail(task.taskId);
            }

            if(task.remove){
                client.requestTaskRemove(task.taskId);
            }
        }

        if(!del.address.equals("") && del.port != 0){
            UdpSender client = UdpSender.start(task.address, task.port);

            if(del.failed){
                client.cleanFailedQueue();
            }
        }
    }

    public static void main(String ... argv) throws IOException {
        CLI cli = new CLI();
        cli.run(cli, argv);
    }
}