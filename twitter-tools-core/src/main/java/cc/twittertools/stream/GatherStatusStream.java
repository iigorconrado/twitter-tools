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

import twitter4j.FilterQuery;
import twitter4j.RawStreamListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public final class GatherStatusStream {
  private static int cnt = 0;
  
  private static final String LANGUAGE_OPTION = "language";
  private static final String LOCATIONS_OPTION = "locations";
  
  @SuppressWarnings("unused")
  private static final String MINUTE_ROLL = ".%d{yyyy-MM-dd-HH-mm}.gz";
  private static final String HOUR_ROLL = ".%d{yyyy-MM-dd-HH}.gz";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws TwitterException {
    Options options = new Options();
    
    options.addOption(OptionBuilder.withArgName("list").hasArg()
        .withDescription("comma-separated list of BCP 47 language identifiers").create(LANGUAGE_OPTION));
    options.addOption(OptionBuilder.withArgName("list").hasArg()
        .withDescription("comma-separated list of longitude,latitude pairs specifying a set of bounding boxes")
        .create(LOCATIONS_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
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

    // creates a custom logger and log messages
    final Logger logger = Logger.getLogger(GatherStatusStream.class);

    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    RawStreamListener rawListener = new RawStreamListener() {

      @Override
      public void onMessage(String rawString) {
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
    
    FilterQuery fq = new FilterQuery();
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      String[] languages = cmdline.getOptionValue(LANGUAGE_OPTION).split(",");
      fq.language(languages);
    }
    if (cmdline.hasOption(LOCATIONS_OPTION)) {
      String[] locations = cmdline.getOptionValue(LOCATIONS_OPTION).split(",");
      double[][] loc = {{Double.parseDouble(locations[0]), Double.parseDouble(locations[1])},
          {Double.parseDouble(locations[2]), Double.parseDouble(locations[3])}};
      fq.locations(loc);
    } else {
      fq.locations(new double[][] { { -180, -90 }, { 180, 90 } });
    }
    twitterStream.addListener(rawListener);
    twitterStream.filter(fq);
  }
}
