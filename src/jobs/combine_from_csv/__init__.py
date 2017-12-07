from shared.context import JobContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as sf

class CombineFromCsvJobContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'combinedDf')

def analyze(sc):
    context = CombineFromCsvJobContext(sc)
    sqlContext = SQLContext(sc)

    #####################################################################################
    #Here we will define the different schemas required to load into separate dataframes
    #we will add some calculated items to the dataframes and the combine them
    #into a single one based on users.
    #####################################################################################
    answersSchema = StructType([ \
        StructField("id", IntegerType(), True), \
        StructField("score", IntegerType(), True), \
        StructField("views", IntegerType(), True), \
        StructField("user_id", IntegerType(), True), \
        StructField("comment_count", IntegerType(), True), \
        StructField("favourite_count", IntegerType(), True), \
        StructField("body_length", IntegerType(), True)])

    badgesSchema = StructType([ \
        StructField("user_id", IntegerType(), True), \
        StructField("number_of_badges", IntegerType(), True)])

    commentsSchema = StructType([ \
        StructField("user_id", IntegerType(), True), \
        StructField("number_of_comments", IntegerType(), True)])

    postLinksSchema = StructType([ \
        StructField("post_id", IntegerType(), True), \
        StructField("number_of_links", IntegerType(), True)])

    questionsSchema = StructType([ \
        StructField("id", IntegerType(), True), \
        StructField("accepted_answer_id", IntegerType(), True), \
        StructField("score", IntegerType(), True), \
        StructField("views", IntegerType(), True), \
        StructField("user_id", IntegerType(), True), \
        StructField("answer_count", IntegerType(), True), \
        StructField("comment_count", IntegerType(), True), \
        StructField("favourite_count", IntegerType(), True), \
        StructField("body_length", IntegerType(), True)])

    usersSchema = StructType([ \
        StructField("id", IntegerType(), True), \
        StructField("reputation", IntegerType(), True), \
        StructField("signup_date", IntegerType(), True), \
        StructField("last_access_date", IntegerType(), True), \
        StructField("has_website", IntegerType(), True), \
        StructField("has_location", IntegerType(), True), \
        StructField("has_about_me", IntegerType(), True), \
        StructField("profile_views", IntegerType(), True), \
        StructField("up_votes", IntegerType(), True), \
        StructField("down_votes", IntegerType(), True), \
        StructField("age", IntegerType(), True)])

    #####################################################################################
    ###  Now we can load the various dataframes and perform intermediate calculations ###
    #####################################################################################
    df_badges = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .load('/user/group-AI/so_badges/output_badges', schema = badgesSchema)

    df_comments = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .load('/user/group-AI/so_comments/output_comments', schema = commentsSchema)

    df_links = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .load('/user/group-AI/so_postlinks/output_postlinks', schema = postLinksSchema)

    answers_df = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .load('/user/group-AI/so_answers/output_answers', schema = answersSchema)

    #####################################################################################
    # for example here we want to summarize the answers df to a user_id level by putting
    # together all the answers attributes grouped for each user_id
    #####################################################################################
    answers_agg_df = answers_df.groupBy("user_id") \
      .agg( \
        sf.sum('score').alias('total_answer_score'), \
        sf.sum('views').alias('total_answer_views'), \
        sf.sum('comment_count').alias('total_answer_comments'), \
        sf.sum('favourite_count').alias('total_answer_favourites'), \
        sf.sum('body_length').alias('total_answer_body_length'), \
        sf.count('id').alias('total_answers'))

    answer_summary = answers_agg_df \
        .withColumn('average_answer_score', answers_agg_df.total_answer_score/answers_agg_df.total_answers) \
        .withColumn('average_answer_body_length', answers_agg_df.total_answer_body_length/answers_agg_df.total_answers) \
        .withColumn('average_answer_comments', answers_agg_df.total_answer_comments/answers_agg_df.total_answers)

    #####################################################################################
    # In the case of questions we will do similar to answers, create intermediate frames
    # and then aggregate in order to create user summaries
    #####################################################################################
    questions_df = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .load('/user/group-AI/so_questions/output_questions', schema = questionsSchema)

    qdf = questions_df \
        .withColumn('has_accepted_answer',  \
            questions_df.accepted_answer_id \
            .cast(BooleanType()) \
            .cast(IntegerType()) \
            )

    questions_agg_df = qdf.groupBy("user_id") \
      .agg( \
        sf.sum('score').alias('total_question_score'), \
        sf.sum('views').alias('total_question_views'), \
        sf.sum('answer_count').alias('total_question_answers'), \
        sf.sum('has_accepted_answer').alias('total_question_accepted_answers'), \
        sf.sum('comment_count').alias('total_question_comments'), \
        sf.sum('favourite_count').alias('total_question_favourites'), \
        sf.sum('body_length').alias('total_question_body_length'), \
        sf.count('id').alias('total_questions'))

    question_summary = questions_agg_df \
        .withColumn('average_question_score', questions_agg_df.total_question_score/questions_agg_df.total_questions) \
        .withColumn('average_question_body_length', questions_agg_df.total_question_body_length/questions_agg_df.total_questions) \
        .withColumn('average_answers_per_question', questions_agg_df.total_question_answers/questions_agg_df.total_questions) \
        .withColumn('average_accepted_answers_per_question', questions_agg_df.total_question_accepted_answers/questions_agg_df.total_questions) \
        .withColumn('average_question_comments', questions_agg_df.total_question_comments/questions_agg_df.total_questions)

    df_users = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .load('/user/group-AI/so_users/output_users', schema = usersSchema)

    #####################################################################################
    # here we will put together our results into one dataframe. We use left outer joins
    # as some users understandably won't have comments, badges, questions etc but we don't
    # want to lose those records.
    #####################################################################################

    master_df = df_users \
        .join(df_badges, df_users.id == df_badges.user_id, 'left_outer') \
        .join(df_comments, df_users.id == df_comments.user_id, 'left_outer') \
        .join(answer_summary, df_users.id == answer_summary.user_id, 'left_outer') \
        .join(question_summary, df_users.id == question_summary.user_id, 'left_outer')

    master_df.select('id' \
        , 'reputation' \
        , 'signup_date' \
        , 'last_access_date' \
        , 'has_website' \
        , 'has_location' \
        , 'has_about_me' \
        , 'profile_views' \
        , 'up_votes' \
        , 'down_votes' \
        , 'age' \
        , 'number_of_badges' \
        , 'number_of_comments' \
        , 'total_answer_score' \
        , 'total_answer_views' \
        , 'total_answer_comments' \
        , 'total_answer_favourites' \
        , 'total_answers' \
        , 'average_answer_score' \
        , 'average_answer_body_length' \
        , 'average_answer_comments' \
        , 'total_question_score' \
        , 'total_question_views' \
        , 'total_question_answers' \
        , 'total_question_accepted_answers' \
        , 'total_question_comments' \
        , 'total_question_favourites' \
        , 'total_questions' \
        , 'average_question_score' \
        , 'average_question_body_length' \
        , 'average_answers_per_question' \
        , 'average_accepted_answers_per_question' \
        , 'average_question_comments') \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .option("header", "true") \
        .mode("overwrite") \
        .save('/user/group-AI/so_file.csv')
