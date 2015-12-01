/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.srvutil;

import java.util.Properties;

import org.apache.commons.cli.*;


/**
 * 只提供Server程序依赖，目的为了拆解客户端依赖，尽可能减少客户端的依赖
 *
 * @author vive
 */
public class ServerUtil {

    public static Options buildCommandlineOptions(final Options options) {
        // -h 参数表示打印帮助信息
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);
        // -n 参数为nameserver 地址和端口信息
        opt = new Option("n", "namesrvAddr", true,
            "Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    /**
     * 将输入的命令行参数解析成CommandLine对象
     *
     * @param appName
     * @param args
     *            命令行参数
     * @param options
     * @param parser
     * @return
     */
    public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
            CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                return null;
            }
        }
        catch (ParseException e) {
            hf.printHelp(appName, options, true);
        }

        return commandLine;
    }


    public static void printCommandLineHelp(final String appName, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }


    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            for (Option opt : opts) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}
