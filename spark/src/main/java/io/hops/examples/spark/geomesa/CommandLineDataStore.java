package io.hops.examples.spark.geomesa;

import org.apache.commons.cli.*;
import org.bouncycastle.util.Arrays;
import org.geotools.data.DataAccessFactory;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CommandLineDataStore {
  private CommandLineDataStore() {
  }
  
  public static Properties createOptions(DataAccessFactory.Param[] parameters) {
    Properties options = new Properties();
    
    System.out.println("Create options");
    
    //        Options options = new Options();
    for (DataAccessFactory.Param p : parameters) {
      if (!p.isDeprecated()) {
        options.setProperty(p.getName(), "");
        //
        //                Option opt = Option.builder()
        //                        .longOpt(p.getName())
        //                        .argName(p.getName())
        //                        .hasArg()
        //                        .desc(p.getDescription().toString())
        //                        .required(p.isRequired())
        //                        .build();
        //                options.addOption(opt);
      }
    }
    
    
    System.out.println(options);
    return options;
  }
  
  public static Properties parseArgs(Class<?> caller, Properties options, String[] args) throws ParseException {
    Properties properties = new Properties();
    System.out.println("Create commands");
    
    Enumeration propertyNames = options.propertyNames();
    while (propertyNames.hasMoreElements()) {
      String option = (String) propertyNames.nextElement();
      if (option.equals("cleanup")) {
        properties.setProperty("cleanup", "true");
        continue;
      }
      
      for (int i = 0; i < args.length - 1; i+=2) {
        if (args[i].replaceAll("--", "").equals(option)) {
          properties.setProperty(option, args[i + 1]);
          break;
        }
      }
    }
    
    
    //            return new DefaultParser().parse(options, args);
    
    //        catch (ParseException e) {
    //        System.err.println(e.getMessage());
    //            HelpFormatter formatter = new HelpFormatter();
    //            formatter.printHelp(caller.getName(), options);
    //            throw e;
    
    System.out.println(properties);
    return properties;
  }
  
  public static Map<String, String> getDataStoreParams(Properties commands) {
    Map<String, String> params = new HashMap<>();
    // noinspection unchecked
    
    Enumeration optionNames = commands.keys();
    
    System.out.println("Get datastore params");
    
    while (optionNames.hasMoreElements()) {
      Object option = optionNames.nextElement();
      Object value = commands.get(option);
      if (value != null) {
        params.put((String) option, (String) value);
      }
    }
    
    System.out.println(params);
    return params;
  }
}
