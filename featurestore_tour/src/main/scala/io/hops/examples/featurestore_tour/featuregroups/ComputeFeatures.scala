package io.hops.examples.featurestore_tour.featuregroups

import com.logicalclocks.hsfs.{DataFormat, HopsworksConnection, TimeTravelFormat, StatisticsConfig}
import com.logicalclocks.hsfs.metadata.validation._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import org.apache.spark.sql.Row

/**
  * Contains logic for computing feature grups for the Hops Feature Store Demo Project
  */
object ComputeFeatures {

  /**
    * Constants
    */
  val TEAMS_DATASET_FILE = "teams.csv"
  val TEAMS_FEATUREGROUP = "teams_features"
  val GAMES_DATASET_FILE = "games.csv"
  val GAMES_FEATUREGROUP = "games_features"
  val SEASON_FEATUREGROUP_TOUR_ON_DEMAND = "season_features_on_demand"
  val GAMES_FEATUREGROUP_TOUR_HUDI = "games_features_hudi_tour"
  val PLAYERS_DATASET_FILE = "players.csv"
  val PLAYERS_FEATUREGROUP = "players_features"
  val ATTENDANCES_DATASET_FILE = "attendances.csv"
  val ATTENDANCES_FEATUREGROUP = "attendances_features"
  val SEASON_SCORES_DATASET_FILE = "season_scores.csv"
  val SEASON_SCORES_FEATUREGROUP = "season_scores_features"
  val TOUR_TRAINING_DATASET = "tour_training_dataset_test"

  val FEATUREGROUP_VERSION = 1
  val SLASH_DELIMITER = "/"

  /**
    * Case classes for parsing the CSV data into typed spark dataframes
    */
  case class RawTeam(
                      team_budget: Double,
                      team_id: Int,
                      team_name: String,
                      team_owner: String,
                      team_position: Int)

  case class TeamFeature(
                          team_budget: Float,
                          team_id: Int,
                          team_position: Int)

  case class RawPlayer(
                        age: Int,
                        rating: Double,
                        team_id: Int,
                        worth: Double)

  case class PlayersTeamFeature(
                                 team_id: Int,
                                 average_player_rating: Float,
                                 average_player_age: Float,
                                 average_player_worth: Float,
                                 sum_player_rating: Float,
                                 sum_player_age: Float,
                                 sum_player_worth: Float)

  case class RawAttendance(
                            attendance: Double,
                            team_id: Int,
                            year: Int)

  case class AttendanceTeamFeature(
                                    team_id: Int,
                                    average_attendance: Float,
                                    sum_attendance: Float)

  case class RawSeasonScore(
                             position: Int,
                             team_id: Int,
                             year: Int)

  case class SeasonScoreTeamFeature(
                                     team_id: Int,
                                     average_position: Float,
                                     sum_position: Float)

  case class RawGame(
                      away_team_id: Int,
                      home_team_id: Int,
                      score: Int)

  val connection = HopsworksConnection.builder.build
  val fs = connection.getFeatureStore

  /**
    * Compute features from games.csv and save to a new feature group called games_features
    *
    * @param spark       the spark session
    * @param log         the spark logger
    * @param datasetPath the path to the dataset where games.csv resides
    */
  def computeGamesFeatureGroup(spark: SparkSession, log: Logger, datasetPath: String): Unit = {
    log.info(s"Computing feature group: ${GAMES_FEATUREGROUP}")
    val input = datasetPath + SLASH_DELIMITER + GAMES_DATASET_FILE
    val rawDf = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(input)
    import spark.implicits._
    val rawDs = rawDf.as[RawGame]
    log.info(s"Creating featuregroup $GAMES_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore ${fs.getName}")
    val games_fg = fs.createFeatureGroup()
      .name(GAMES_FEATUREGROUP)
      .version(FEATUREGROUP_VERSION)
      .description("Features of games")
      .timeTravelFormat(TimeTravelFormat.NONE)
      .primaryKeys(Seq("home_team_id"))
      .statisticsConfig(new StatisticsConfig(true, true, true, true))
      .build()
    games_fg.save(rawDs.toDF)
    log.info(s"Creation of featuregroup $GAMES_FEATUREGROUP complete")

    val expectationGamesScore = (fs.createExpectation()
      .rules(Seq(
        Rule.createRule(RuleName.HAS_MAX).min(1).max(1).level(Level.WARNING).build(),
        Rule.createRule(RuleName.HAS_MIN).min(0).max(0).level(Level.ERROR).build()))
      .name("games score")
      .description("min/max score")
      .features(Seq("score"))
      .build())
    expectationGamesScore.save()

    log.info(s"Creating hudi featuregroup $GAMES_FEATUREGROUP_TOUR_HUDI version $FEATUREGROUP_VERSION in " +
      s"featurestore ${fs.getName}")
    val partitionCols = List("score")
    val hudi_fg = fs.createFeatureGroup()
      .name(GAMES_FEATUREGROUP_TOUR_HUDI)
      .version(FEATUREGROUP_VERSION)
      .description("Features of games, HUDI feature group example")
      .timeTravelFormat(TimeTravelFormat.HUDI)
      .primaryKeys(Seq("home_team_id"))
      .partitionKeys(partitionCols)
      .validationType(ValidationType.ALL)
      .expectations(Seq(expectationGamesScore))
      .statisticsConfig(new StatisticsConfig(true, true, true, true))
      .build()
    hudi_fg.save(rawDs.toDF)
    log.info(s"Creation Hudi featuregroup $GAMES_FEATUREGROUP_TOUR_HUDI complete")
  }

  /**
    * Compute features from season_scores.csv and save to a new feature group called season_scores_features
    *
    * @param spark       the spark session
    * @param log         the spark logger
    * @param datasetPath the path to the dataset where season_scores.csv resides
    */
  def computeSeasonScoresFeatureGroup(spark: SparkSession, log: Logger, datasetPath: String): Unit = {
    log.info(s"Computing feature group: ${SEASON_SCORES_FEATUREGROUP}")
    val input = datasetPath + SLASH_DELIMITER + SEASON_SCORES_DATASET_FILE
    val rawDf = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(input)
    import spark.implicits._
    val rawDs = rawDf.as[RawSeasonScore]
    val sum = rawDs.groupBy("team_id").sum()
    val count = rawDs.groupBy("team_id").count()
    val rawFeaturesDf = sum.join(count, "team_id")
    val featureDs = rawFeaturesDf.map((row: Row) => {
      val sumPosition = row.getAs[Long]("sum(position)")
      val count = row.getAs[Long]("count")
      val avgPosition = sumPosition.toFloat / count.toFloat
      val teamId = row.getAs[Int]("team_id")
      new SeasonScoreTeamFeature(
        team_id = teamId,
        average_position = avgPosition,
        sum_position = sumPosition)
    })

    log.info(s"Creating featuregroup $SEASON_SCORES_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore ${fs.getName}")
    val scores_fg = fs.createFeatureGroup()
      .name(SEASON_SCORES_FEATUREGROUP)
      .version(FEATUREGROUP_VERSION)
      .description("Features of average season scores for football teams")
      .timeTravelFormat(TimeTravelFormat.NONE)
      .onlineEnabled(true)
      .primaryKeys(Seq("team_id"))
      .statisticsConfig(new StatisticsConfig(true, true, true, true))
      .build()
    scores_fg.save(featureDs.toDF)
    log.info(s"Creation of featuregroup $SEASON_SCORES_FEATUREGROUP complete")

    log.info(s"Creating on-demand featuregroup $SEASON_FEATUREGROUP_TOUR_ON_DEMAND version $FEATUREGROUP_VERSION in " +
      s"featurestore ${fs.getName}")
  }

  def computeSeasonScoresOnDemandFeatureGroup(log: Logger): Unit = {
    val storageConnector =
      fs.getStorageConnector(System.getenv("HADOOP_USER_NAME").replace("__", "_") + "_onlinefeaturestore");
    val onDmdFg = fs.createOnDemandFeatureGroup()
      .name(SEASON_FEATUREGROUP_TOUR_ON_DEMAND)
      .description("Features of games, on demand feature group example")
      .version(FEATUREGROUP_VERSION)
      .query("SELECT * FROM season_scores_features_1 WHERE average_position > 3")
      .storageConnector(storageConnector)
      .build()
    onDmdFg.save()
    log.info(s"Creation of on-demand featuregroup $SEASON_FEATUREGROUP_TOUR_ON_DEMAND complete")
  }

  /**
    * Compute features from attendances.csv and save to a new feature group called attendances_features
    *
    * @param spark       the spark session
    * @param log         the spark logger
    * @param datasetPath the path to the dataset where attendances.csv resides
    */
  def computeAttendanceFeatureGroup(spark: SparkSession, log: Logger, datasetPath: String): Unit = {
    log.info(s"Computing feature group: ${ATTENDANCES_FEATUREGROUP}")
    val input = datasetPath + SLASH_DELIMITER + ATTENDANCES_DATASET_FILE
    val rawDf = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(input)
    import spark.implicits._
    val rawDs = rawDf.as[RawAttendance]
    val sum = rawDs.groupBy("team_id").sum()
    val count = rawDs.groupBy("team_id").count()
    val rawFeaturesDf = sum.join(count, "team_id")
    val featureDs = rawFeaturesDf.map((row: Row) => {
      val sumAttendance = row.getAs[Double]("sum(attendance)")
      val count = row.getAs[Long]("count")
      val avgAttendance = sumAttendance.toFloat / count.toFloat
      val teamId = row.getAs[Int]("team_id")
      new AttendanceTeamFeature(
        team_id = teamId,
        average_attendance = avgAttendance,
        sum_attendance = sumAttendance.toFloat)
    })
    log.info(s"Creating expectations for $ATTENDANCES_FEATUREGROUP")
    val expectationAttendance = (fs.createExpectation()
      .rules(Seq(
        Rule.createRule(RuleName.HAS_MAX).min(1999999).max(9999999).level(Level.WARNING).build(),
        Rule.createRule(RuleName.IS_POSITIVE).min(1).max(1).level(Level.ERROR).build()))
      .name("attendance")
      .description("min/max and is_positive attendance validations")
      .features(Seq("average_attendance", "sum_attendance"))
      .build())
    expectationAttendance.save()

    val expectationAttendanceTeamsId = (fs.createExpectation()
      .rules(Seq(
        Rule.createRule(RuleName.HAS_NUMBER_OF_DISTINCT_VALUES).min(1).max(1).level(Level.ERROR).build()))
      .name("attendance_teams")
      .description("team id distinct values")
      .features(Seq("team_id"))
      .build())
    expectationAttendanceTeamsId.save()

    log.info(s"Creating featuregroup $ATTENDANCES_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore ${fs.getName}")
    val attendances_fg = fs.createFeatureGroup()
      .name(ATTENDANCES_FEATUREGROUP)
      .version(FEATUREGROUP_VERSION)
      .description("Features of average attendance of games of football teams")
      .timeTravelFormat(TimeTravelFormat.NONE)
      .primaryKeys(Seq("team_id"))
      .statisticsConfig(new StatisticsConfig(true, true, true, true))
      .validationType(ValidationType.ALL)
      .expectations(Seq(expectationAttendance, expectationAttendanceTeamsId))
      .build()
    attendances_fg.save(featureDs.toDF)
    log.info(s"Creation of featuregroup $ATTENDANCES_FEATUREGROUP complete")
  }

  /**
    * Compute features from players.csv and save to a new feature group called players_features
    *
    * @param spark       the spark session
    * @param log         the spark logger
    * @param datasetPath the path to the dataset where players.csv resides
    */
  def computePlayersFeatureGroup(spark: SparkSession, log: Logger, datasetPath: String): Unit = {
    log.info(s"Computing feature group: ${PLAYERS_FEATUREGROUP}")
    val input = datasetPath + SLASH_DELIMITER + PLAYERS_DATASET_FILE
    val rawDf = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(input)
    import spark.implicits._
    val rawDs = rawDf.as[RawPlayer]
    val sum = rawDs.groupBy("team_id").sum()
    val count = rawDs.groupBy("team_id").count()
    val rawFeaturesDf = sum.join(count, "team_id")
    val featureDs = rawFeaturesDf.map((row: Row) => {
      val sumAge = row.getAs[Long]("sum(age)")
      val sumRating = row.getAs[Double]("sum(rating)")
      val sumWorth = row.getAs[Double]("sum(worth)")
      val count = row.getAs[Long]("count")
      val avgAge = sumAge.toFloat / count.toFloat
      val avgRating = sumRating.toFloat / count.toFloat
      val avgWorth = sumWorth.toFloat / count.toFloat
      val teamId = row.getAs[Int]("team_id")
      new PlayersTeamFeature(
        team_id = teamId,
        average_player_rating = avgRating,
        average_player_age = avgAge,
        average_player_worth = avgWorth,
        sum_player_rating = sumRating.toFloat,
        sum_player_age = sumAge,
        sum_player_worth = sumWorth.toFloat)
    })
    log.info(s"Creating featuregroup $PLAYERS_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore ${fs.getName}")
    val players_fg = fs.createFeatureGroup()
      .name(PLAYERS_FEATUREGROUP)
      .version(FEATUREGROUP_VERSION)
      .description("Aggregate features of players football teams")
      .timeTravelFormat(TimeTravelFormat.NONE)
      .primaryKeys(Seq("team_id"))
      .statisticsConfig(new StatisticsConfig(true, true, true, true))
      .build()
    players_fg.save(featureDs.toDF)
    log.info(s"Creation of featuregroup $PLAYERS_FEATUREGROUP complete")
  }

  /**
    * Compute features from teams.csv and save to a new feature group called teams_features
    *
    * @param spark       the spark session
    * @param log         the spark logger
    * @param datasetPath the path to the dataset where teams.csv resides
    */
  def computeTeamsFeatureGroup(spark: SparkSession, log: Logger, datasetPath: String): Unit = {
    log.info(s"Computing feature group: ${TEAMS_FEATUREGROUP}")
    val input = datasetPath + SLASH_DELIMITER + TEAMS_DATASET_FILE
    val rawDf = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(input)
    import spark.implicits._
    val rawDs = rawDf.as[RawTeam]
    val featureDs = rawDs.map((rawTeam: RawTeam) => TeamFeature(team_budget = rawTeam.team_budget.toFloat, team_id = rawTeam.team_id, team_position = rawTeam.team_position))
    val description = "Features of football teams"
    val primaryKey = "team_id"
    val jobName: String = null
    log.info(s"Creating featuregroup $TEAMS_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore ${fs.getName}")
    val teams_fg = fs.createFeatureGroup()
      .name(TEAMS_FEATUREGROUP)
      .version(FEATUREGROUP_VERSION)
      .description("Features of football teams")
      .timeTravelFormat(TimeTravelFormat.NONE)
      .primaryKeys(Seq("team_id"))
      .statisticsConfig(new StatisticsConfig(true, true, true, true))
      .build()
    teams_fg.save(featureDs.toDF)
    log.info(s"Creation of featuregroup $TEAMS_FEATUREGROUP complete")
  }

  /**
    * Creates a sample training dataset for the tour
    */
  def createTrainingDataset(log: Logger): Unit = {
    log.info(s"Creating Training Dataset: $TOUR_TRAINING_DATASET")
    val players_fg = fs.getFeatureGroup(PLAYERS_FEATUREGROUP)
    val teams_fg = fs.getFeatureGroup(TEAMS_FEATUREGROUP)
    val attendance_fg = fs.getFeatureGroup(ATTENDANCES_FEATUREGROUP)
    val query = players_fg.select(List("average_player_age"))
      .join(teams_fg.select(List("team_budget")))
      .join(attendance_fg.select(List("average_attendance")))
    val td = fs.createTrainingDataset()
      .name("tour_training_dataset_test")
      .version(1)
      .description("Sample Training Dataset for the Feature store Tour")
      .dataFormat(DataFormat.TFRECORD)
      .statisticsConfig(new StatisticsConfig(true, true, true, true))
      .build()
    td.save(query)
  }

}
