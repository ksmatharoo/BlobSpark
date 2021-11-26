package notused;

import org.apache.commons.cli.*;

import java.util.Properties;

/**
notused.Driver -config
src/main/resources/config.properties
-properties
src/main/resources/env.properties
-sparkConf
src/main/resources/sparkConf.conf
*/

public class Driver {

    public static void main(String[] args) throws Exception {
        Options options = OptionsParser.buildOptions();
        CommandLineParser parser = new DefaultParser();

        CommandLine cmd  = parser.parse(options,args,false);
        Properties properties = OptionsParser.retrieveAllProperties(cmd,null);

        System.out.println(properties);

    }
}
