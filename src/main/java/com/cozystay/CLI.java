package com.cozystay;

import com.beust.jcommander.*;
import com.cozystay.command.UdpSender;

import java.io.IOException;

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
        @Parameter(names =  {"--reset"}, description = "reset syncTask to primary queue", order = 3)
        private boolean reset = false;
        @Parameter(names =  {"--remove"}, description = "remove syncTask from it's queue", order = 4)
        private boolean remove = false;
    }

    public void run(CLI cli, String[] argv) throws IOException {
        CommandStatus status = new CommandStatus();
        CommandTask task = new CommandTask();

        JCommander jCommander = JCommander.newBuilder()
                .addObject(cli)
                .addCommand("status", status)
                .addCommand("task", task)
                .build();

        jCommander.parse(argv);

        if(help){
            jCommander.setProgramName("Data Sync CLI");
            jCommander.usage();
            return;
        }

        if(version){
            JCommander.getConsole().println("1.0.0");
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

            if(task.reset){
                client.requestTaskReset(task.taskId);
            }

            if(task.remove){
                client.requestTaskRemove(task.taskId);
            }
        }
    }

    public static void main(String ... argv) throws IOException {
        CLI cli = new CLI();
        cli.run(cli, argv);
    }
}