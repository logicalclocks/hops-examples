package io.hops.examples.featurestore.featuregroups

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import io.hops.util.Hops
import scala.collection.JavaConversions._
import collection.JavaConverters._
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
  val PLAYERS_DATASET_FILE = "players.csv"
  val PLAYERS_FEATUREGROUP = "players_features"
  val ATTENDANCES_DATASET_FILE = "attendances.csv"
  val ATTENDANCES_FEATUREGROUP = "attendances_features"
  val SEASON_SCORES_DATASET_FILE = "season_scores.csv"
  val SEASON_SCORES_FEATUREGROUP = "season_scores_features"

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

  val featurestore = Hops.getProjectFeaturestore
  val descriptiveStats = true
  val featureCorr = true
  val featureHistograms = true
  val clusterAnalysis = true
  val statColumns = List[String]().asJava
  val numBins = 20
  val corrMethod = "pearson"
  val numClusters = 5

  /**
    * Compute features from games.csv and save to a new feature group called games_features
    *
    * @param spark the spark session
    * @param log the spark logger
    * @param datasetPath the path to the dataset where games.csv resides
    */
  def computeGamesFeatureGroup(spark: SparkSession, log: Logger, datasetPath: String): Unit = {
    log.info(s"Computing feature group: ${GAMES_FEATUREGROUP}")
    val input = datasetPath + SLASH_DELIMITER + GAMES_DATASET_FILE
    val rawDf = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(input)
    import spark.implicits._
    val rawDs = rawDf.as[RawGame]
    val dependencies = List[String](input).asJava
    val description = "Features of average season scores for football teams"
    val primaryKey = "home_team_id"
    log.info(s"Creating featuregroup $GAMES_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore $featurestore")
    Hops.createFeaturegroup(spark, rawDs.toDF, GAMES_FEATUREGROUP, featurestore, FEATUREGROUP_VERSION, description,
      null, dependencies, primaryKey, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, corrMethod, numClusters)
    log.info(s"Creation of featuregroup $GAMES_FEATUREGROUP complete")
  }

  /**
    * Compute features from season_scores.csv and save to a new feature group called season_scores_features
    *
    * @param spark the spark session
    * @param log the spark logger
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
    val dependencies = List[String](input).asJava
    val description = "Features of average season scores for football teams"
    val primaryKey = "team_id"
    log.info(s"Creating featuregroup $SEASON_SCORES_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore $featurestore")
    Hops.createFeaturegroup(spark, featureDs.toDF, SEASON_SCORES_FEATUREGROUP, featurestore, FEATUREGROUP_VERSION, description,
      null, dependencies, primaryKey, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, corrMethod, numClusters)
    log.info(s"Creation of featuregroup $SEASON_SCORES_FEATUREGROUP complete")
  }

  /**
    * Compute features from attendances.csv and save to a new feature group called attendances_features
    *
    * @param spark the spark session
    * @param log the spark logger
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
    val dependencies = List[String](input).asJava
    val description = "Features of average attendance of games of football teams"
    val primaryKey = "team_id"
    log.info(s"Creating featuregroup $ATTENDANCES_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore $featurestore")
    Hops.createFeaturegroup(spark, featureDs.toDF, ATTENDANCES_FEATUREGROUP, featurestore, FEATUREGROUP_VERSION, description,
      null, dependencies, primaryKey, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, corrMethod, numClusters)
    log.info(s"Creation of featuregroup $ATTENDANCES_FEATUREGROUP complete")
  }

  /**
    * Compute features from players.csv and save to a new feature group called players_features
    *
    * @param spark the spark session
    * @param log the spark logger
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
    val dependencies = List[String](input).asJava
    val description = "Aggregate features of players football teams"
    val primaryKey = "team_id"
    log.info(s"Creating featuregroup $PLAYERS_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore $featurestore")
    Hops.createFeaturegroup(spark, featureDs.toDF, PLAYERS_FEATUREGROUP, featurestore, FEATUREGROUP_VERSION, description,
      null, dependencies, primaryKey, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, corrMethod, numClusters)
    log.info(s"Creation of featuregroup $PLAYERS_FEATUREGROUP complete")
  }

  /**
    * Compute features from teams.csv and save to a new feature group called teams_features
    *
    * @param spark the spark session
    * @param log the spark logger
    * @param datasetPath the path to the dataset where teams.csv resides
    */
  def computeTeamsFeatureGroup(spark: SparkSession, log: Logger, datasetPath: String): Unit = {
    log.info(s"Computing feature group: ${TEAMS_FEATUREGROUP}")
    val input = datasetPath + SLASH_DELIMITER + TEAMS_DATASET_FILE
    val rawDf = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(input)
    import spark.implicits._
    val rawDs = rawDf.as[RawTeam]
    val featureDs = rawDs.map((rawTeam: RawTeam) => TeamFeature(team_budget = rawTeam.team_budget.toFloat, team_id = rawTeam.team_id, team_position = rawTeam.team_position))
    val dependencies = List[String](input).asJava
    val description = "Features of football teams"
    val primaryKey = "team_id"
    log.info(s"Creating featuregroup $TEAMS_FEATUREGROUP version $FEATUREGROUP_VERSION in featurestore $featurestore")
    Hops.createFeaturegroup(spark, featureDs.toDF, TEAMS_FEATUREGROUP, featurestore, FEATUREGROUP_VERSION, description,
      null, dependencies, primaryKey, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, corrMethod, numClusters)
    log.info(s"Creation of featuregroup $TEAMS_FEATUREGROUP complete")
  }

}
