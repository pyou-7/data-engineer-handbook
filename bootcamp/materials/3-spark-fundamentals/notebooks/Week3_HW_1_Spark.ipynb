{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 156,
   "id": "4ae455cf-b8b5-4ae9-92ef-fe76ea82a874",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import broadcast, sum, countDistinct, col, avg, count, max, row_number\n",
    "from pyspark.sql.window import Window"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 83,
   "id": "5a18885b-c310-4a55-a3c6-5dadec60c493",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "spark = SparkSession.builder.appName(\"Spark Homework\").getOrCreate()\n",
    "spark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", \"-1\")\n",
    "\n",
    "match_details = spark.read.option(\"header\", \"true\") \\\n",
    "                    .option(\"inferSchema\", \"true\") \\\n",
    "                    .csv(\"/home/iceberg/data/match_details.csv\")\n",
    "matches = spark.read.option(\"header\", \"true\") \\\n",
    "                    .option(\"inferSchema\", \"true\") \\\n",
    "                    .csv(\"/home/iceberg/data/matches.csv\")\n",
    "medals_matches_players = spark.read.option(\"header\", \"true\") \\\n",
    "                    .option(\"inferSchema\", \"true\") \\\n",
    "                    .csv(\"/home/iceberg/data/medals_matches_players.csv\")\n",
    "medals = spark.read.option(\"header\", \"true\") \\\n",
    "                    .option(\"inferSchema\", \"true\") \\\n",
    "                    .csv(\"/home/iceberg/data/medals.csv\")\n",
    "maps = spark.read.option(\"header\", \"true\") \\\n",
    "                    .option(\"inferSchema\", \"true\") \\\n",
    "                    .csv(\"/home/iceberg/data/maps.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "079be2da-0eb4-499e-932d-17a512a445e0",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------------+\n",
      "|            match_id|               mapid|is_team_game|         playlist_id|     game_variant_id|is_match_over|     completion_date|match_duration|game_mode|      map_variant_id|\n",
      "+--------------------+--------------------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------------+\n",
      "|11de1a94-8d07-416...|c7edbf0f-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-02-22 00:00:...|          NULL|     NULL|                NULL|\n",
      "|d3643e71-3e51-43e...|cb914b9e-f206-11e...|       false|d0766624-dbd7-453...|257a305e-4dd3-41f...|         true|2016-02-14 00:00:...|          NULL|     NULL|                NULL|\n",
      "|d78d2aae-36e4-48a...|c7edbf0f-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-03-24 00:00:...|          NULL|     NULL|55e5ee2e-88df-465...|\n",
      "|b440069e-ec5f-4f5...|c7edbf0f-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2015-12-23 00:00:...|          NULL|     NULL|ec3eef73-13e3-4d4...|\n",
      "|1dd475fc-ee6b-4e1...|c93d708f-f206-11e...|        true|0e39ead4-383b-445...|42f97cca-2cb4-497...|         true|2016-04-07 00:00:...|          NULL|     NULL|                NULL|\n",
      "+--------------------+--------------------+------------+--------------------+--------------------+-------------+--------------------+--------------+---------+--------------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "matches.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "b7867fa6-ae91-4111-90ba-dc297e356027",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+---------------+----------+-----+\n",
      "|            match_id|player_gamertag|  medal_id|count|\n",
      "+--------------------+---------------+----------+-----+\n",
      "|009fdac5-e15c-47c...|       EcZachly|3261908037|    7|\n",
      "|009fdac5-e15c-47c...|       EcZachly| 824733727|    2|\n",
      "|009fdac5-e15c-47c...|       EcZachly|2078758684|    2|\n",
      "|009fdac5-e15c-47c...|       EcZachly|2782465081|    2|\n",
      "|9169d1a3-955c-4ea...|       EcZachly|3001183151|    1|\n",
      "+--------------------+---------------+----------+-----+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "medals_matches_players.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "5d79ca60-6575-489f-9dc7-caeb9433fedd",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+--------------------+-----------+----------+------------------+-------------------+------------+-------------+--------------+--------------------+--------------+----------+\n",
      "|  medal_id|          sprite_uri|sprite_left|sprite_top|sprite_sheet_width|sprite_sheet_height|sprite_width|sprite_height|classification|         description|          name|difficulty|\n",
      "+----------+--------------------+-----------+----------+------------------+-------------------+------------+-------------+--------------+--------------------+--------------+----------+\n",
      "|2315448068|                NULL|       NULL|      NULL|              NULL|               NULL|        NULL|         NULL|          NULL|                NULL|          NULL|      NULL|\n",
      "|3565441934|                NULL|       NULL|      NULL|              NULL|               NULL|        NULL|         NULL|          NULL|                NULL|          NULL|      NULL|\n",
      "|4162659350|https://content.h...|        750|       750|                74|                 74|        1125|          899|      Breakout|Kill the last ene...| Buzzer Beater|        45|\n",
      "|1573153198|https://content.h...|          0|       300|                74|                 74|        1125|          899|      Breakout|Survive a one-on-...|    Vanquisher|        30|\n",
      "| 298813630|https://content.h...|          0|       825|                74|                 74|        1125|          899|         Style|Kill an enemy wit...|Spartan Charge|       135|\n",
      "+----------+--------------------+-----------+----------+------------------+-------------------+------------+-------------+--------------+--------------------+--------------+----------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "medals.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "7317f94a-18ea-4626-8ced-b5e740979a73",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/01/16 11:04:16 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+\n",
      "|            match_id|player_gamertag|previous_spartan_rank|spartan_rank|previous_total_xp|total_xp|previous_csr_tier|previous_csr_designation|previous_csr|previous_csr_percent_to_next_tier|previous_csr_rank|current_csr_tier|current_csr_designation|current_csr|current_csr_percent_to_next_tier|current_csr_rank|player_rank_on_team|player_finished|player_average_life|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_shots_landed|player_total_melee_kills|player_total_melee_damage|player_total_assassinations|player_total_ground_pound_kills|player_total_shoulder_bash_kills|player_total_grenade_damage|player_total_power_weapon_damage|player_total_power_weapon_grabs|player_total_deaths|player_total_assists|player_total_grenade_kills|did_win|team_id|\n",
      "+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+\n",
      "|71d79b23-4143-435...|      taterbase|                    5|           5|            12537|   13383|                1|                       3|           0|                               98|             NULL|               2|                      3|          0|                              26|            NULL|                  4|          false|        PT14.81149S|                 6|                     4|                       255|                       28|                       0|                        0|                          0|                              0|                               0|                          0|                               0|                              0|                 13|                   1|                         0|      1|      1|\n",
      "|71d79b23-4143-435...| SuPeRSaYaInG0D|                   18|          18|           131943|  132557|                2|                       3|           0|                                2|             NULL|               1|                      3|          0|                              76|            NULL|                  7|          false|      PT11.2990845S|                 7|                     3|        350.58792304992676|                       49|                       1|                       45|                          0|                              0|                               0|                          0|                               0|                              0|                 18|                   2|                         0|      0|      0|\n",
      "|71d79b23-4143-435...|       EcZachly|                   21|          21|           168811|  169762|                2|                       5|           0|                               94|             NULL|               3|                      5|          0|                              24|            NULL|                  3|          false|      PT19.1357063S|                12|                    12|                       625|                       43|                       0|                        0|                          0|                              0|                               0|                          0|                               0|                              0|                 10|                   4|                         0|      1|      1|\n",
      "|71d79b23-4143-435...|    johnsnake04|                   14|          14|            64073|   64639|             NULL|                    NULL|        NULL|                             NULL|             NULL|            NULL|                   NULL|       NULL|                            NULL|            NULL|                  6|          false|      PT21.1521599S|                13|                    13|                       605|                       24|                       0|                        0|                          0|                              0|                               0|                          0|                               0|                              0|                  9|                   2|                         0|      0|      0|\n",
      "|71d79b23-4143-435...| Super Mac Bros|                   26|          26|           243425|  244430|                1|                       5|           0|                               86|             NULL|               2|                      5|          0|                               8|            NULL|                  2|          false|      PT12.8373793S|                13|                    12|                       595|                       32|                       1|       20.004501342773438|                          0|                              0|                               0|                          0|                               0|                              0|                 15|                   2|                         0|      1|      1|\n",
      "+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "match_details.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 84,
   "id": "1a797f5e-41e1-4058-b75d-a2f5cc173cd3",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+-------------------+--------------------+\n",
      "|               mapid|               name|         description|\n",
      "+--------------------+-------------------+--------------------+\n",
      "|c93d708f-f206-11e...|              Urban|Andesia was the c...|\n",
      "|cb251c51-f206-11e...|     Raid on Apex 7|This unbroken rin...|\n",
      "|c854e54f-f206-11e...|March on Stormbreak|                NULL|\n",
      "|c8d69870-f206-11e...| Escape from A.R.C.|Scientists flocke...|\n",
      "|73ed1fd0-45e5-4bb...|             Osiris|                NULL|\n",
      "+--------------------+-------------------+--------------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "maps.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 88,
   "id": "0e44df07-2e0d-4925-89db-344628e43eda",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Broadcast JOIN medals and maps\n",
    "broadcasted_medals = broadcast(medals.alias(\"med\"))\n",
    "broadcasted_maps = broadcast(maps.alias(\"map\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 86,
   "id": "8b034cf6-6d21-40ec-9432-35f9dc228eee",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "DataFrame[]"
      ]
     },
     "execution_count": 86,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"DROP TABLE bootcamp.bucketed_matches\")\n",
    "spark.sql(\"DROP TABLE bootcamp.bucketed_match_details\")\n",
    "spark.sql(\"DROP TABLE bootcamp.bucketed_medals_matches_players\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 87,
   "id": "6087504e-17cb-48cb-a641-b120fd365072",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# Bucket match_details, matches, and medals_matches_players on match_id with 16 buckets\n",
    "matches.write.bucketBy(16, \"match_id\").saveAsTable(\"bootcamp.bucketed_matches\")\n",
    "match_details.write.bucketBy(16, \"match_id\").saveAsTable(\"bootcamp.bucketed_match_details\")\n",
    "medals_matches_players.write.bucketBy(16, \"match_id\").saveAsTable(\"bootcamp.bucketed_medals_matches_players\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 89,
   "id": "5d956da8-38f8-4f3a-9576-d94ac4f8fd72",
   "metadata": {},
   "outputs": [],
   "source": [
    "bucketed_matches = spark.read.table(\"bootcamp.bucketed_matches\")\n",
    "bucketed_match_details = spark.read.table(\"bootcamp.bucketed_match_details\")\n",
    "bucketed_medals_matches_players = spark.read.table(\"bootcamp.bucketed_medals_matches_players\")\n",
    "\n",
    "df_final = bucketed_medals_matches_players.alias(\"mp\") \\\n",
    "    .join(bucketed_matches.alias(\"m\"), col(\"mp.match_id\") == col(\"m.match_id\"), \"left\") \\\n",
    "    .join(bucketed_match_details.alias(\"md\"), col(\"mp.match_id\") == col(\"md.match_id\"), \"left\") \\\n",
    "    .join(broadcasted_medals, col(\"mp.medal_id\") == col(\"med.medal_id\"), \"left\") \\\n",
    "    .join(broadcasted_maps, col(\"m.mapid\") == col(\"map.mapid\"), \"left\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 90,
   "id": "8e15f1c2-90ef-4285-aac4-a86275842ca3",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+---------------+---------+-----+--------------------+--------------------+------------+--------------------+--------------------+-------------+-------------------+--------------+---------+--------------------+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+---------+--------------------+-----------+----------+------------------+-------------------+------------+-------------+--------------+--------------------+-----------+----------+--------------------+-------+--------------------+\n",
      "|            match_id|player_gamertag| medal_id|count|            match_id|               mapid|is_team_game|         playlist_id|     game_variant_id|is_match_over|    completion_date|match_duration|game_mode|      map_variant_id|            match_id|player_gamertag|previous_spartan_rank|spartan_rank|previous_total_xp|total_xp|previous_csr_tier|previous_csr_designation|previous_csr|previous_csr_percent_to_next_tier|previous_csr_rank|current_csr_tier|current_csr_designation|current_csr|current_csr_percent_to_next_tier|current_csr_rank|player_rank_on_team|player_finished|player_average_life|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_shots_landed|player_total_melee_kills|player_total_melee_damage|player_total_assassinations|player_total_ground_pound_kills|player_total_shoulder_bash_kills|player_total_grenade_damage|player_total_power_weapon_damage|player_total_power_weapon_grabs|player_total_deaths|player_total_assists|player_total_grenade_kills|did_win|team_id| medal_id|          sprite_uri|sprite_left|sprite_top|sprite_sheet_width|sprite_sheet_height|sprite_width|sprite_height|classification|         description|       name|difficulty|               mapid|   name|         description|\n",
      "+--------------------+---------------+---------+-----+--------------------+--------------------+------------+--------------------+--------------------+-------------+-------------------+--------------+---------+--------------------+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+---------+--------------------+-----------+----------+------------------+-------------------+------------+-------------+--------------+--------------------+-----------+----------+--------------------+-------+--------------------+\n",
      "|27d7c16b-b780-4f8...|       EcZachly|824733727|    1|27d7c16b-b780-4f8...|c7805740-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-02-20 00:00:00|          NULL|     NULL|b5f6104e-3a99-438...|27d7c16b-b780-4f8...|    Westy3hunna|                   62|          63|          1319629| 1320858|             NULL|                    NULL|        NULL|                             NULL|             NULL|            NULL|                   NULL|       NULL|                            NULL|            NULL|                  6|          false|      PT37.9222484S|                 5|                     3|         520.9089546203613|                       36|                       0|                      0.0|                          0|                              0|                               0|          17.71877098083496|                             0.0|                              0|                  6|                   1|                         0|      0|      1|824733727|https://content.h...|        975|       450|                74|                 74|        1125|          899|         Style|Distract an oppon...|Distraction|       150|c7805740-f206-11e...|Glacier|Each of Halo's mi...|\n",
      "|27d7c16b-b780-4f8...|       EcZachly|824733727|    1|27d7c16b-b780-4f8...|c7805740-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-02-20 00:00:00|          NULL|     NULL|b5f6104e-3a99-438...|27d7c16b-b780-4f8...|    ILLICIT 117|                  101|         101|          3313090| 3314382|                2|                       5|           0|                               96|             NULL|               2|                      5|          0|                              64|            NULL|                  7|          false|      PT35.5613927S|                 5|                     4|         438.8812189102173|                       20|                       0|                      0.0|                          0|                              0|                               1|                        0.0|                             0.0|                              0|                  6|                   0|                         0|      0|      1|824733727|https://content.h...|        975|       450|                74|                 74|        1125|          899|         Style|Distract an oppon...|Distraction|       150|c7805740-f206-11e...|Glacier|Each of Halo's mi...|\n",
      "|27d7c16b-b780-4f8...|       EcZachly|824733727|    1|27d7c16b-b780-4f8...|c7805740-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-02-20 00:00:00|          NULL|     NULL|b5f6104e-3a99-438...|27d7c16b-b780-4f8...|   Ragin Trajan|                   85|          85|          2449979| 2451630|                5|                       5|           0|                               30|             NULL|               5|                      5|          0|                              54|            NULL|                  2|          false|      PT39.4164357S|                 6|                     4|         676.5214672088623|                       42|                       1|        55.55000305175781|                          0|                              0|                               0|          133.7152042388916|                             0.0|                              0|                  5|                   2|                         0|      1|      0|824733727|https://content.h...|        975|       450|                74|                 74|        1125|          899|         Style|Distract an oppon...|Distraction|       150|c7805740-f206-11e...|Glacier|Each of Halo's mi...|\n",
      "|27d7c16b-b780-4f8...|       EcZachly|824733727|    1|27d7c16b-b780-4f8...|c7805740-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-02-20 00:00:00|          NULL|     NULL|b5f6104e-3a99-438...|27d7c16b-b780-4f8...|  TheRaginRhino|                   69|          69|          1703526| 1705034|                5|                       5|           0|                               56|             NULL|               5|                      5|          0|                              80|            NULL|                  4|          false|      PT37.9500643S|                 2|                     0|         568.5477042198181|                       45|                       0|                      0.0|                          0|                              0|                               0|          153.0685305595398|                             0.0|                              0|                  5|                   3|                         2|      1|      0|824733727|https://content.h...|        975|       450|                74|                 74|        1125|          899|         Style|Distract an oppon...|Distraction|       150|c7805740-f206-11e...|Glacier|Each of Halo's mi...|\n",
      "|27d7c16b-b780-4f8...|       EcZachly|824733727|    1|27d7c16b-b780-4f8...|c7805740-f206-11e...|        true|f72e0ef0-7c4a-430...|1e473914-46e4-408...|         true|2016-02-20 00:00:00|          NULL|     NULL|b5f6104e-3a99-438...|27d7c16b-b780-4f8...|       EcZachly|                   92|          92|          2742321| 2743658|                4|                       5|           0|                               50|             NULL|               4|                      5|          0|                              18|            NULL|                  5|          false|      PT45.9357397S|                 7|                     5|         770.7586822509766|                       69|                       1|       29.999998092651367|                          0|                              0|                               0|         14.925763130187988|                             0.0|                              0|                  5|                   0|                         0|      0|      1|824733727|https://content.h...|        975|       450|                74|                 74|        1125|          899|         Style|Distract an oppon...|Distraction|       150|c7805740-f206-11e...|Glacier|Each of Halo's mi...|\n",
      "+--------------------+---------------+---------+-----+--------------------+--------------------+------------+--------------------+--------------------+-------------+-------------------+--------------+---------+--------------------+--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+---------+--------------------+-----------+----------+------------------+-------------------+------------+-------------+--------------+--------------------+-----------+----------+--------------------+-------+--------------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df_final.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 94,
   "id": "48dc572b-f7e5-4432-ac7c-d7d6535f9dfa",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 321:============================>                            (4 + 4) / 8]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------------+------------------+\n",
      "|player_gamertag|avg_kills_per_game|\n",
      "+---------------+------------------+\n",
      "|   gimpinator14|             109.0|\n",
      "+---------------+------------------+\n",
      "only showing top 1 row\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# Which player averages the most kills per game?\n",
    "most_kills_per_game = df_final.groupBy(\"md.player_gamertag\").agg(\n",
    "    avg(\"md.player_total_kills\").alias(\"avg_kills_per_game\")\n",
    ").orderBy(col(\"avg_kills_per_game\").desc())\n",
    "most_kills_per_game.show(1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 95,
   "id": "7346d4f2-41c1-45a7-ad8b-b6160c703138",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------+\n",
      "|         playlist_id|matches_played|\n",
      "+--------------------+--------------+\n",
      "|f72e0ef0-7c4a-430...|          7640|\n",
      "+--------------------+--------------+\n",
      "only showing top 1 row\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# Which playlist gets played the most?\n",
    "most_played_playlist = df_final.groupBy(\"m.playlist_id\").agg(\n",
    "    countDistinct(\"m.match_id\").alias(\"matches_played\")\n",
    ").orderBy(col(\"matches_played\").desc())\n",
    "most_played_playlist.show(1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 109,
   "id": "2a9f4f9d-56d9-499d-a38f-bc61396c0e63",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 495:======================================>                  (4 + 2) / 6]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------+--------------+\n",
      "|               mapid|          name|matches_played|\n",
      "+--------------------+--------------+--------------+\n",
      "|c7edbf0f-f206-11e...|Breakout Arena|          7032|\n",
      "+--------------------+--------------+--------------+\n",
      "only showing top 1 row\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# Which map gets played the most?\n",
    "most_played_map = df_final.groupBy(\"m.mapid\", \"map.name\").agg(\n",
    "    countDistinct(\"m.match_id\").alias(\"matches_played\")\n",
    ").orderBy(col(\"matches_played\").desc())\n",
    "most_played_map.show(1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 158,
   "id": "dd9fe7eb-92e3-4385-ab90-e948d2d812db",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/01/17 12:36:50 WARN HintErrorLogger: A join hint (strategy=broadcast) is specified but it is not part of a join relation.\n",
      "25/01/17 12:36:51 WARN DataSourceV2Strategy: Can't translate true to source filter, unsupported expression\n",
      "25/01/17 12:36:51 WARN DataSourceV2Strategy: Can't translate true to source filter, unsupported expression\n",
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------+-------------------+\n",
      "|               mapid|          name|killing_spree_count|\n",
      "+--------------------+--------------+-------------------+\n",
      "|c7edbf0f-f206-11e...|Breakout Arena|               5100|\n",
      "+--------------------+--------------+-------------------+\n",
      "only showing top 1 row\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Which map do players get the most Killing Spree medals on?\n",
    "killing_spree_medal_id = broadcasted_medals.filter(col(\"name\") == \"Killing Spree\").select(\"medal_id\").first()[\"medal_id\"]\n",
    "window_spec = Window.partitionBy(\"map.mapid\", \"map.name\", \"m.match_id\").orderBy(col(\"mp.count\").desc())\n",
    "\n",
    "df_with_max = df_final.filter(col(\"med.medal_id\") == killing_spree_medal_id).withColumn(\n",
    "    \"rank\", row_number().over(window_spec)\n",
    ")\n",
    "\n",
    "result = df_with_max.filter(col(\"rank\") == 1).drop(\"rank\")\n",
    "\n",
    "most_killing_spree_medals = result.filter(col(\"med.medal_id\") == killing_spree_medal_id) \\\n",
    ".groupBy(\"map.mapid\", \"map.name\").agg(\n",
    "    sum(\"mp.count\").alias(\"killing_spree_count\")\n",
    ").orderBy(col(\"killing_spree_count\").desc())\n",
    "most_killing_spree_medals.show(1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 160,
   "id": "0811647c-4629-462d-8c00-1e21ae355934",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "== Physical Plan ==\n",
      "AdaptiveSparkPlan isFinalPlan=false\n",
      "+- Sort [playlist_id#5182 ASC NULLS FIRST], false, 0\n",
      "   +- Exchange hashpartitioning(playlist_id#5182, 10), REPARTITION_BY_NUM, [plan_id=37364]\n",
      "      +- BroadcastHashJoin [mapid#5180], [mapid#5003], LeftOuter, BuildRight, false\n",
      "         :- BroadcastHashJoin [medal_id#5273L], [medal_id#4962L], LeftOuter, BuildRight, false\n",
      "         :  :- SortMergeJoin [match_id#5271], [match_id#5199], LeftOuter\n",
      "         :  :  :- SortMergeJoin [match_id#5271], [match_id#5179], LeftOuter\n",
      "         :  :  :  :- Sort [match_id#5271 ASC NULLS FIRST], false, 0\n",
      "         :  :  :  :  +- Exchange hashpartitioning(match_id#5271, 200), ENSURE_REQUIREMENTS, [plan_id=37348]\n",
      "         :  :  :  :     +- BatchScan demo.bootcamp.bucketed_medals_matches_players[match_id#5271, player_gamertag#5272, medal_id#5273L, count#5274] demo.bootcamp.bucketed_medals_matches_players (branch=null) [filters=, groupedBy=] RuntimeFilters: []\n",
      "         :  :  :  +- Sort [match_id#5179 ASC NULLS FIRST], false, 0\n",
      "         :  :  :     +- Exchange hashpartitioning(match_id#5179, 200), ENSURE_REQUIREMENTS, [plan_id=37349]\n",
      "         :  :  :        +- BatchScan demo.bootcamp.bucketed_matches[match_id#5179, mapid#5180, is_team_game#5181, playlist_id#5182, game_variant_id#5183, is_match_over#5184, completion_date#5185, match_duration#5186, game_mode#5187, map_variant_id#5188] demo.bootcamp.bucketed_matches (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "         :  :  +- Sort [match_id#5199 ASC NULLS FIRST], false, 0\n",
      "         :  :     +- Exchange hashpartitioning(match_id#5199, 200), ENSURE_REQUIREMENTS, [plan_id=37355]\n",
      "         :  :        +- BatchScan demo.bootcamp.bucketed_match_details[match_id#5199, player_gamertag#5200, previous_spartan_rank#5201, spartan_rank#5202, previous_total_xp#5203, total_xp#5204, previous_csr_tier#5205, previous_csr_designation#5206, previous_csr#5207, previous_csr_percent_to_next_tier#5208, previous_csr_rank#5209, current_csr_tier#5210, current_csr_designation#5211, current_csr#5212, current_csr_percent_to_next_tier#5213, current_csr_rank#5214, player_rank_on_team#5215, player_finished#5216, player_average_life#5217, player_total_kills#5218, player_total_headshots#5219, player_total_weapon_damage#5220, player_total_shots_landed#5221, player_total_melee_kills#5222, ... 12 more fields] demo.bootcamp.bucketed_match_details (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "         :  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=37359]\n",
      "         :     +- Filter isnotnull(medal_id#4962L)\n",
      "         :        +- FileScan csv [medal_id#4962L,sprite_uri#4963,sprite_left#4964,sprite_top#4965,sprite_sheet_width#4966,sprite_sheet_height#4967,sprite_width#4968,sprite_height#4969,classification#4970,description#4971,name#4972,difficulty#4973] Batched: false, DataFilters: [isnotnull(medal_id#4962L)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/medals.csv], PartitionFilters: [], PushedFilters: [IsNotNull(medal_id)], ReadSchema: struct<medal_id:bigint,sprite_uri:string,sprite_left:int,sprite_top:int,sprite_sheet_width:int,sp...\n",
      "         +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=37362]\n",
      "            +- Filter isnotnull(mapid#5003)\n",
      "               +- FileScan csv [mapid#5003,name#5004,description#5005] Batched: false, DataFilters: [isnotnull(mapid#5003)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/maps.csv], PartitionFilters: [], PushedFilters: [IsNotNull(mapid)], ReadSchema: struct<mapid:string,name:string,description:string>\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Sort within partitions\n",
    "playlist_sorted_df = df_final.repartition(10, col(\"m.playlist_id\"))\\\n",
    "    .sortWithinPartitions(col(\"m.playlist_id\"))\n",
    "playlist_sorted_df.explain()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 161,
   "id": "d0ce873f-7e71-4bf5-ab6c-1708c9c5e017",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "== Physical Plan ==\n",
      "AdaptiveSparkPlan isFinalPlan=false\n",
      "+- Sort [mapid#5003 ASC NULLS FIRST], false, 0\n",
      "   +- Exchange hashpartitioning(mapid#5003, 10), REPARTITION_BY_NUM, [plan_id=37439]\n",
      "      +- BroadcastHashJoin [mapid#5180], [mapid#5003], LeftOuter, BuildRight, false\n",
      "         :- BroadcastHashJoin [medal_id#5273L], [medal_id#4962L], LeftOuter, BuildRight, false\n",
      "         :  :- SortMergeJoin [match_id#5271], [match_id#5199], LeftOuter\n",
      "         :  :  :- SortMergeJoin [match_id#5271], [match_id#5179], LeftOuter\n",
      "         :  :  :  :- Sort [match_id#5271 ASC NULLS FIRST], false, 0\n",
      "         :  :  :  :  +- Exchange hashpartitioning(match_id#5271, 200), ENSURE_REQUIREMENTS, [plan_id=37423]\n",
      "         :  :  :  :     +- BatchScan demo.bootcamp.bucketed_medals_matches_players[match_id#5271, player_gamertag#5272, medal_id#5273L, count#5274] demo.bootcamp.bucketed_medals_matches_players (branch=null) [filters=, groupedBy=] RuntimeFilters: []\n",
      "         :  :  :  +- Sort [match_id#5179 ASC NULLS FIRST], false, 0\n",
      "         :  :  :     +- Exchange hashpartitioning(match_id#5179, 200), ENSURE_REQUIREMENTS, [plan_id=37424]\n",
      "         :  :  :        +- BatchScan demo.bootcamp.bucketed_matches[match_id#5179, mapid#5180, is_team_game#5181, playlist_id#5182, game_variant_id#5183, is_match_over#5184, completion_date#5185, match_duration#5186, game_mode#5187, map_variant_id#5188] demo.bootcamp.bucketed_matches (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "         :  :  +- Sort [match_id#5199 ASC NULLS FIRST], false, 0\n",
      "         :  :     +- Exchange hashpartitioning(match_id#5199, 200), ENSURE_REQUIREMENTS, [plan_id=37430]\n",
      "         :  :        +- BatchScan demo.bootcamp.bucketed_match_details[match_id#5199, player_gamertag#5200, previous_spartan_rank#5201, spartan_rank#5202, previous_total_xp#5203, total_xp#5204, previous_csr_tier#5205, previous_csr_designation#5206, previous_csr#5207, previous_csr_percent_to_next_tier#5208, previous_csr_rank#5209, current_csr_tier#5210, current_csr_designation#5211, current_csr#5212, current_csr_percent_to_next_tier#5213, current_csr_rank#5214, player_rank_on_team#5215, player_finished#5216, player_average_life#5217, player_total_kills#5218, player_total_headshots#5219, player_total_weapon_damage#5220, player_total_shots_landed#5221, player_total_melee_kills#5222, ... 12 more fields] demo.bootcamp.bucketed_match_details (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "         :  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=37434]\n",
      "         :     +- Filter isnotnull(medal_id#4962L)\n",
      "         :        +- FileScan csv [medal_id#4962L,sprite_uri#4963,sprite_left#4964,sprite_top#4965,sprite_sheet_width#4966,sprite_sheet_height#4967,sprite_width#4968,sprite_height#4969,classification#4970,description#4971,name#4972,difficulty#4973] Batched: false, DataFilters: [isnotnull(medal_id#4962L)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/medals.csv], PartitionFilters: [], PushedFilters: [IsNotNull(medal_id)], ReadSchema: struct<medal_id:bigint,sprite_uri:string,sprite_left:int,sprite_top:int,sprite_sheet_width:int,sp...\n",
      "         +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=37437]\n",
      "            +- Filter isnotnull(mapid#5003)\n",
      "               +- FileScan csv [mapid#5003,name#5004,description#5005] Batched: false, DataFilters: [isnotnull(mapid#5003)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/maps.csv], PartitionFilters: [], PushedFilters: [IsNotNull(mapid)], ReadSchema: struct<mapid:string,name:string,description:string>\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "map_sorted_df = df_final.repartition(10, col(\"map.mapid\"))\\\n",
    "    .sortWithinPartitions(col(\"map.mapid\"))\n",
    "\n",
    "# Save results\n",
    "map_sorted_df.explain()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 163,
   "id": "4e0fed86-c629-4923-bc74-5e93d86b6a25",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "== Physical Plan ==\n",
      "AdaptiveSparkPlan isFinalPlan=false\n",
      "+- Sort [playlist_id#5182 ASC NULLS FIRST], true, 0\n",
      "   +- Exchange rangepartitioning(playlist_id#5182 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=37516]\n",
      "      +- Exchange hashpartitioning(mapid#5003, 10), REPARTITION_BY_NUM, [plan_id=37514]\n",
      "         +- BroadcastHashJoin [mapid#5180], [mapid#5003], LeftOuter, BuildRight, false\n",
      "            :- BroadcastHashJoin [medal_id#5273L], [medal_id#4962L], LeftOuter, BuildRight, false\n",
      "            :  :- SortMergeJoin [match_id#5271], [match_id#5199], LeftOuter\n",
      "            :  :  :- SortMergeJoin [match_id#5271], [match_id#5179], LeftOuter\n",
      "            :  :  :  :- Sort [match_id#5271 ASC NULLS FIRST], false, 0\n",
      "            :  :  :  :  +- Exchange hashpartitioning(match_id#5271, 200), ENSURE_REQUIREMENTS, [plan_id=37498]\n",
      "            :  :  :  :     +- BatchScan demo.bootcamp.bucketed_medals_matches_players[match_id#5271, player_gamertag#5272, medal_id#5273L, count#5274] demo.bootcamp.bucketed_medals_matches_players (branch=null) [filters=, groupedBy=] RuntimeFilters: []\n",
      "            :  :  :  +- Sort [match_id#5179 ASC NULLS FIRST], false, 0\n",
      "            :  :  :     +- Exchange hashpartitioning(match_id#5179, 200), ENSURE_REQUIREMENTS, [plan_id=37499]\n",
      "            :  :  :        +- BatchScan demo.bootcamp.bucketed_matches[match_id#5179, mapid#5180, is_team_game#5181, playlist_id#5182, game_variant_id#5183, is_match_over#5184, completion_date#5185, match_duration#5186, game_mode#5187, map_variant_id#5188] demo.bootcamp.bucketed_matches (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "            :  :  +- Sort [match_id#5199 ASC NULLS FIRST], false, 0\n",
      "            :  :     +- Exchange hashpartitioning(match_id#5199, 200), ENSURE_REQUIREMENTS, [plan_id=37505]\n",
      "            :  :        +- BatchScan demo.bootcamp.bucketed_match_details[match_id#5199, player_gamertag#5200, previous_spartan_rank#5201, spartan_rank#5202, previous_total_xp#5203, total_xp#5204, previous_csr_tier#5205, previous_csr_designation#5206, previous_csr#5207, previous_csr_percent_to_next_tier#5208, previous_csr_rank#5209, current_csr_tier#5210, current_csr_designation#5211, current_csr#5212, current_csr_percent_to_next_tier#5213, current_csr_rank#5214, player_rank_on_team#5215, player_finished#5216, player_average_life#5217, player_total_kills#5218, player_total_headshots#5219, player_total_weapon_damage#5220, player_total_shots_landed#5221, player_total_melee_kills#5222, ... 12 more fields] demo.bootcamp.bucketed_match_details (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "            :  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=37509]\n",
      "            :     +- Filter isnotnull(medal_id#4962L)\n",
      "            :        +- FileScan csv [medal_id#4962L,sprite_uri#4963,sprite_left#4964,sprite_top#4965,sprite_sheet_width#4966,sprite_sheet_height#4967,sprite_width#4968,sprite_height#4969,classification#4970,description#4971,name#4972,difficulty#4973] Batched: false, DataFilters: [isnotnull(medal_id#4962L)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/medals.csv], PartitionFilters: [], PushedFilters: [IsNotNull(medal_id)], ReadSchema: struct<medal_id:bigint,sprite_uri:string,sprite_left:int,sprite_top:int,sprite_sheet_width:int,sp...\n",
      "            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=37512]\n",
      "               +- Filter isnotnull(mapid#5003)\n",
      "                  +- FileScan csv [mapid#5003,name#5004,description#5005] Batched: false, DataFilters: [isnotnull(mapid#5003)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/maps.csv], PartitionFilters: [], PushedFilters: [IsNotNull(mapid)], ReadSchema: struct<mapid:string,name:string,description:string>\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "playlist_global_sort = df_final.repartition(10, col(\"map.mapid\"))\\\n",
    "    .sort(col(\"m.playlist_id\"))\n",
    "playlist_global_sort.explain()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 164,
   "id": "c06ca552-0b1c-4ecf-b389-257a79f28641",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "== Physical Plan ==\n",
      "AdaptiveSparkPlan isFinalPlan=false\n",
      "+- Sort [mapid#5003 ASC NULLS FIRST], true, 0\n",
      "   +- Exchange rangepartitioning(mapid#5003 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=37593]\n",
      "      +- Exchange hashpartitioning(mapid#5003, 10), REPARTITION_BY_NUM, [plan_id=37591]\n",
      "         +- BroadcastHashJoin [mapid#5180], [mapid#5003], LeftOuter, BuildRight, false\n",
      "            :- BroadcastHashJoin [medal_id#5273L], [medal_id#4962L], LeftOuter, BuildRight, false\n",
      "            :  :- SortMergeJoin [match_id#5271], [match_id#5199], LeftOuter\n",
      "            :  :  :- SortMergeJoin [match_id#5271], [match_id#5179], LeftOuter\n",
      "            :  :  :  :- Sort [match_id#5271 ASC NULLS FIRST], false, 0\n",
      "            :  :  :  :  +- Exchange hashpartitioning(match_id#5271, 200), ENSURE_REQUIREMENTS, [plan_id=37575]\n",
      "            :  :  :  :     +- BatchScan demo.bootcamp.bucketed_medals_matches_players[match_id#5271, player_gamertag#5272, medal_id#5273L, count#5274] demo.bootcamp.bucketed_medals_matches_players (branch=null) [filters=, groupedBy=] RuntimeFilters: []\n",
      "            :  :  :  +- Sort [match_id#5179 ASC NULLS FIRST], false, 0\n",
      "            :  :  :     +- Exchange hashpartitioning(match_id#5179, 200), ENSURE_REQUIREMENTS, [plan_id=37576]\n",
      "            :  :  :        +- BatchScan demo.bootcamp.bucketed_matches[match_id#5179, mapid#5180, is_team_game#5181, playlist_id#5182, game_variant_id#5183, is_match_over#5184, completion_date#5185, match_duration#5186, game_mode#5187, map_variant_id#5188] demo.bootcamp.bucketed_matches (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "            :  :  +- Sort [match_id#5199 ASC NULLS FIRST], false, 0\n",
      "            :  :     +- Exchange hashpartitioning(match_id#5199, 200), ENSURE_REQUIREMENTS, [plan_id=37582]\n",
      "            :  :        +- BatchScan demo.bootcamp.bucketed_match_details[match_id#5199, player_gamertag#5200, previous_spartan_rank#5201, spartan_rank#5202, previous_total_xp#5203, total_xp#5204, previous_csr_tier#5205, previous_csr_designation#5206, previous_csr#5207, previous_csr_percent_to_next_tier#5208, previous_csr_rank#5209, current_csr_tier#5210, current_csr_designation#5211, current_csr#5212, current_csr_percent_to_next_tier#5213, current_csr_rank#5214, player_rank_on_team#5215, player_finished#5216, player_average_life#5217, player_total_kills#5218, player_total_headshots#5219, player_total_weapon_damage#5220, player_total_shots_landed#5221, player_total_melee_kills#5222, ... 12 more fields] demo.bootcamp.bucketed_match_details (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []\n",
      "            :  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=37586]\n",
      "            :     +- Filter isnotnull(medal_id#4962L)\n",
      "            :        +- FileScan csv [medal_id#4962L,sprite_uri#4963,sprite_left#4964,sprite_top#4965,sprite_sheet_width#4966,sprite_sheet_height#4967,sprite_width#4968,sprite_height#4969,classification#4970,description#4971,name#4972,difficulty#4973] Batched: false, DataFilters: [isnotnull(medal_id#4962L)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/medals.csv], PartitionFilters: [], PushedFilters: [IsNotNull(medal_id)], ReadSchema: struct<medal_id:bigint,sprite_uri:string,sprite_left:int,sprite_top:int,sprite_sheet_width:int,sp...\n",
      "            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=37589]\n",
      "               +- Filter isnotnull(mapid#5003)\n",
      "                  +- FileScan csv [mapid#5003,name#5004,description#5005] Batched: false, DataFilters: [isnotnull(mapid#5003)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/maps.csv], PartitionFilters: [], PushedFilters: [IsNotNull(mapid)], ReadSchema: struct<mapid:string,name:string,description:string>\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "map_global_sort = df_final.repartition(10, col(\"map.mapid\"))\\\n",
    "    .sort(col(\"map.mapid\"))\n",
    "map_global_sort.explain()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "8697bf3e-e770-4c3b-89bd-888aa521ea48",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.18"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
