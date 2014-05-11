/**
 * Twitter Tools
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cc.twittertools.stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.rolling.RollingFileAppender;
import org.apache.log4j.rolling.TimeBasedRollingPolicy;
import org.apache.log4j.varia.LevelRangeFilter;

import cc.twittertools.search.api.RunQueriesThrift;
import twitter4j.FilterQuery;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.RawStreamListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public final class GatherStatusStream {
  private static int cnt = 0;
  
  private static final String LANGUAGE_OPTION = "language";
  private static final String LOCATIONS_OPTION = "locations";
  private static final String NO_BOUNDING_BOX_OPTION = "no-bounding-box";
  
  @SuppressWarnings("unused")
  private static final String MINUTE_ROLL = ".%d{yyyy-MM-dd-HH-mm}.gz";
  private static final String HOUR_ROLL = ".%d{yyyy-MM-dd-HH}.gz";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws TwitterException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("list")
        .hasArgs()
        .withDescription("comma-separated list of BCP 47 language identifiers")
        .withLongOpt(LANGUAGE_OPTION)
        .create('l'));
    options.addOption(OptionBuilder.withArgName("list")
        .hasArgs()
        .withDescription("comma-separated list of longitude,latitude pairs specifying a set of bounding boxes")
        .withLongOpt(LOCATIONS_OPTION)
        .create('g'));
    options.addOption("n", NO_BOUNDING_BOX_OPTION, false, "do not consider places' bounding box");
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(RunQueriesThrift.class.getName(), options);
      System.exit(-1);
    }
    
    PatternLayout layoutStandard = new PatternLayout();
    layoutStandard.setConversionPattern("[%p] %d %c %M - %m%n");

    PatternLayout layoutSimple = new PatternLayout();
    layoutSimple.setConversionPattern("%m%n");

    // Filter for the statuses: we only want INFO messages
    LevelRangeFilter filter = new LevelRangeFilter();
    filter.setLevelMax(Level.INFO);
    filter.setLevelMin(Level.INFO);
    filter.setAcceptOnMatch(true);
    filter.activateOptions();

    TimeBasedRollingPolicy statusesRollingPolicy = new TimeBasedRollingPolicy();
    statusesRollingPolicy.setFileNamePattern("statuses.log" + HOUR_ROLL);
    statusesRollingPolicy.activateOptions();

    RollingFileAppender statusesAppender = new RollingFileAppender();
    statusesAppender.setRollingPolicy(statusesRollingPolicy);
    statusesAppender.addFilter(filter);
    statusesAppender.setLayout(layoutSimple);
    statusesAppender.activateOptions();

    TimeBasedRollingPolicy warningsRollingPolicy = new TimeBasedRollingPolicy();
    warningsRollingPolicy.setFileNamePattern("warnings.log" + HOUR_ROLL);
    warningsRollingPolicy.activateOptions();

    RollingFileAppender warningsAppender = new RollingFileAppender();
    warningsAppender.setRollingPolicy(statusesRollingPolicy);
    warningsAppender.setThreshold(Level.WARN);
    warningsAppender.setLayout(layoutStandard);
    warningsAppender.activateOptions();

    ConsoleAppender consoleAppender = new ConsoleAppender();
    consoleAppender.setThreshold(Level.WARN);
    consoleAppender.setLayout(layoutStandard);
    consoleAppender.activateOptions();

    // configures the root logger
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.INFO);
    rootLogger.removeAllAppenders();
    rootLogger.addAppender(consoleAppender);
    rootLogger.addAppender(statusesAppender);
    rootLogger.addAppender(warningsAppender);

    // creates filters for the query
    FilterQuery fq = new FilterQuery();
    StringBuilder criteria = new StringBuilder();
    
    /*
     * @see https://dev.twitter.com/docs/streaming-apis/parameters#language
     */
    final boolean filterLanguage = cmdline.hasOption(LANGUAGE_OPTION);
    String[] languages = null;
    if (filterLanguage) {
      languages = cmdline.getOptionValue(LANGUAGE_OPTION).split(",");
      fq.language(languages);
      criteria.append("languages: [" + cmdline.getOptionValue(LANGUAGE_OPTION) + "]\t");
    }
    final String[] langs = languages;
    
    /*
     * @see https://dev.twitter.com/docs/streaming-apis/parameters#locations
     */
    double[][] locations = null;
    if (cmdline.hasOption(LOCATIONS_OPTION)) {
      String[] locationsArg = cmdline.getOptionValue(LOCATIONS_OPTION).split(",");
      int nCoords = locationsArg.length; 
      if (nCoords % 2 == 0) {
        int pairs = nCoords/2;
        locations = new double[pairs][2];
        int cnt = 0;
        for (int i=0; i<pairs;i++){
          locations[i][0] = Double.parseDouble(locationsArg[cnt]);
          cnt++;
          locations[i][1] = Double.parseDouble(locationsArg[cnt]);
          cnt++;
        }
        fq.locations(locations);
        criteria.append("locations: [" + cmdline.getOptionValue(LOCATIONS_OPTION) + "]\t");
      } else {
        System.err.println("There is a missing coordinate. See "
            + "https://dev.twitter.com/docs/streaming-apis/parameters#locations");
        System.exit(-1);
      }
    } else {
      fq.locations(new double[][] { { -180, -90 }, { 180, 90 } });
    }
    final double[][] loc = locations;
    
    final boolean no_bounding_box = cmdline.hasOption(NO_BOUNDING_BOX_OPTION);
    if (no_bounding_box){
      criteria.append("--no-bounding-box\t");
    }
    
    // creates a custom logger and log messages
    final Logger logger = Logger.getLogger(GatherStatusStream.class);
    
    logger.info(criteria);
    
    RawStreamListener rawListener = new RawStreamListener() {

      @Override
      public void onMessage(String rawString) {
        if (no_bounding_box && loc != null){
          try {
            JSONObject status = new JSONObject(rawString);
            JSONObject coordObj = status.getJSONObject("coordinates");
            JSONArray coords = coordObj.getJSONArray("coordinates");
            double longitude = coords.getDouble(0);
            double latitude = coords.getDouble(1);
            
            // checks location
            for (int i=0; i<loc.length;i+=2){
              if (((loc[i][0] <= longitude) && (longitude <= loc[i+1][0])) ||
                  ((loc[i][1] <= latitude) && (latitude <= loc[i+1][1]))){
                break;
              } else if (i == loc.length - 1) return;
            }
          } catch (JSONException e) { /* Either "Coordinates" is null or trash is coming*/
            return;
          }
        }
        
        if (filterLanguage){
          try {
            JSONObject status = new JSONObject(rawString);
            // checks language
            String lang = status.getString("lang");
            for (int i=0; i<langs.length; i++){
              if (langs[i].equals(lang)) break;
              else if (i == langs.length - 1) return;
            }
          } catch (JSONException e) { /* Trash is coming */
            return;
          }
        }
        cnt++;
        logger.info(rawString);
        if (cnt % 1000 == 0) {
          System.out.println(cnt + " messages received.");
        }
      }

      @Override
      public void onException(Exception ex) {
        logger.warn(ex);
      }
    };
    
    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addListener(rawListener);
    twitterStream.filter(fq);
  }
}
